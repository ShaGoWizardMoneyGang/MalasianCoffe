package joiner

import (
	"fmt"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery4 struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaUsersInput    *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery4  *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func createUserMap(userReceiver packet.PacketReceiver) map[string]string {
	storePkt := userReceiver.GetPayload()
	lines := strings.Split(storePkt, "\n")
	lines = lines[:len(lines)-1]

	// Le damos un tamano inicial de lines porque deberia tener un tamano igual
	// al de la cantidad de lineas. Ademas, ya pre-alocamos la memoria.
	storeID2Name := make(map[string]string, len(lines))

	for _, line := range lines {
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeID2Name[store_id] = store_name
	}

	return storeID2Name
}

func joinQuery4(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	storeReceiver := packet.NewPacketReceiver("Store")

	userReceiver := packet.NewPacketReceiver("User")

	// Aca me guardo todos los packets de transactions que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	transactionReceiver := packet.NewPacketReceiver("Transactions")

	// Resultado final
	var joinedTransactions strings.Builder

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pkt := <-inputChannel

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "stores" {
			storeReceiver.ReceivePacket(pkt)
		} else if dataset_name == "users" {
			userReceiver.ReceivePacket(pkt)
		} else if dataset_name == "transactions" {
			transactionReceiver.ReceivePacket(pkt)

		} else {
			bitacora.Error(fmt.Sprintf("JoinerQuery4 received packet from dataset that was not expecting: %s", dataset_name))
		}

		// No joineamos hasta tenerlo todo.
		if storeReceiver.ReceivedAll() && userReceiver.ReceivedAll() && transactionReceiver.ReceivedAll() {
			// jq4.logger.Info("Comienza proceso de join")
			joinerFunctionQuery4(storeReceiver, userReceiver, transactionReceiver, &joinedTransactions)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"users", "stores", "transactions"}, []string{joinedTransactions.String()})

	// Liberamos
	joinedTransactions.Reset()

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}
}

func (jq4 *joinerQuery4) Build(rabbitAddr string) {
	jq4.inputChannel          = make(chan packet.Packet)

	jq4.outputChannel         = make(chan packet.Packet)

	jq4.colaStoresInput       = colas.InstanceQueue("FilteredStores4", rabbitAddr)
	jq4.colaUsersInput        = colas.InstanceQueue("FilteredUsers4", rabbitAddr)
	jq4.colaAggTransInput     = colas.InstanceQueue("GlobalAggregation4", rabbitAddr)

	jq4.colaSalidaQuery4      = colas.InstanceQueue("SalidaQuery4", rabbitAddr)

	jq4.sessionHandler        = sessionhandler.NewSessionHandler(joinQuery4, jq4.outputChannel)

}

func (jq4 *joinerQuery4) Process() {
	go colas.InputQueue(jq4.colaStoresInput, jq4.inputChannel)

	go colas.InputQueue(jq4.colaUsersInput, jq4.inputChannel)

	go colas.InputQueue(jq4.colaAggTransInput, jq4.inputChannel)

	for {
		select {
		case inputPacket := <-jq4.inputChannel:
			jq4.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq4.outputChannel:
			jq4.colaSalidaQuery4.Send(packetJoineado)
		}
	}
}

// package main

// import (
// 	"bytes"
// 	"fmt"
// 	"log/jq4.logger"
// 	"malasian_coffe/packets/packet"
// 	"malasian_coffe/system/middleware"
// 	"malasian_coffe/utils/network"
// 	"os"
// 	"strings"
// )

// type joinerOptions func(*Joiner, string) string

// type Joiner struct {
// 	Function  joinerOptions
// 	Stores    map[string]string // aca me voy a guardar en memoria las tablas chica
// 	MenuItems map[string]string
// }

// // Recibe year_half_created_at, store_id, tpv
// // joinea con stores.csv cargado en memoria con store_id, store_name
// // y me devuelve year_half_created_at, store_name, tpv
/*func joinerFunctionQuery4(inputTransaction *strings.Builder, storeMap map[string]string, userMap map[string]string, joinedResult *strings.Builder) {
	input := inputTransaction.String()

	// Liberamos el buffer de input
	inputTransaction.Reset()

	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 2 {
			panic("No hay 3 columnas como se esperaba")
		}
		// transactionID, storeID, userID
		// TODO: EN ESTA PORONGA EL USER ID ES UN FLOAT
		storeID, userID := cols[0], cols[1]
		storeName    := storeMap[storeID]
		userBirthday, exits := userMap[userID]
		if !exits {
			userBirthday = "2002-12-08"
		}

		// Necesito algo del estilo: storeName, birthday
		fmt.Fprintf(joinedResult, "%s,%s\n", storeName, userBirthday)
	}
}*/

func joinerFunctionQuery4(storeReceiver packet.PacketReceiver, userReceiver packet.PacketReceiver, transactionReceiver packet.PacketReceiver, joinedTransactions *strings.Builder) {
	userMap := createUserMap(userReceiver)
	storeMap := createStoreMap(storeReceiver)

	transactions := transactionReceiver.GetPayload()
	lines := strings.Split(transactions, "\n")
	lines = lines[:len(lines)-1]
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 2 {
			panic("No hay 3 columnas como se esperaba")
		}
		// transactionID, storeID, userID
		// TODO: EN ESTA PORONGA EL USER ID ES UN FLOAT
		storeID, userID := cols[0], cols[1]
		storeName := storeMap[storeID]
		userBirthday, exits := userMap[userID]
		if !exits {
			userBirthday = "2002-12-08"
		}

		// Necesito algo del estilo: storeName, birthday
		fmt.Fprintf(joinedTransactions, "%s,%s\n", storeName, userBirthday)
	}
}

// // Recibe year_month_created_at, item_id, quantity
// // joinea con stores.csv cargado en memoria con item_id, item_name
// // y me devuelve year_month_created_at, item_name, quantity
// func (j *Joiner) joinerFunctionQuery2Quantity(input string) string {
// 	lines := strings.Split(input, "\n")
// 	lines = lines[:len(lines)-1]
// 	var b strings.Builder
// 	for _, r := range lines {
// 		cols := strings.Split(r, ",")
// 		if len(cols) < 3 {
// 			panic("No hay 3 columnas como se esperaba")
// 		}
// 		month, itemID, quantity := cols[0], cols[1], cols[2]
// 		itemName := j.MenuItems[itemID]
// 		fmt.Fprintf(&b, "%s,%s,%s\n", month, itemName, quantity)
// 	}
// 	return b.String()
// }

// // Recibe year_month_created_at, item_id, subtotal
// // joinea con menu_items.csv cargado en memoria con item_id, item_name
// // y me devuelve year_month_created_at, item_name, subtotal
// func (j *Joiner) joinerFunctionQuery2Subtotal(input string) string {
// 	lines := strings.Split(input, "\n")
// 	lines = lines[:len(lines)-1]
// 	var b strings.Builder
// 	for _, r := range lines {
// 		cols := strings.Split(r, ",")
// 		if len(cols) < 3 {
// 			panic("No hay 3 columnas como se esperaba")
// 		}
// 		month, itemID, quantity := cols[0], cols[1], cols[2]
// 		storeName := j.MenuItems[itemID]
// 		fmt.Fprintf(&b, "%s,%s,%s\n", month, storeName, quantity)
// 	}
// 	return b.String()
// }

// func (c *Joiner) Process(pkt packet.Packet, function string) []packet.Packet {
// 	input := pkt.GetPayload()
// 	function_name := strings.ToLower(function)

// 	var output string
// 	switch function_name {
// 	case "query3":
// 		output = c.joinerFunctionQuery4(input)
// 	case "query2quantity":
// 		output = c.joinerFunctionQuery2Quantity(input)
// 	case "query2subtotal":
// 		output = c.joinerFunctionQuery2Subtotal(input)
// 	default:
// 		panic(fmt.Sprintf("Unknwon function %s", function))
// 	}

// 	outputs := []string{output}
// 	new_packet := packet.ChangePayload(pkt, outputs)

// 	return new_packet
// }
// 	joinFunction := os.Args[2]
// 	if len(joinFunction) == 0 {
// 		panic("No join function provided")
// 	}
// 	rabbitAddr := os.Args[1]

// 	switch joinFunction {
// 	case "Query4":
// 		colaEntradaStores, err := middleware.CreateQueue("FilteredStores", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
// 		if err != nil {
// 			panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
// 		}
// 		msgQueue, consumeError := colaEntradaStores.StartConsuming()
// 		if consumeError != 0 {
// 			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
// 		}
// 		worker := Joiner{}
// 		worker.Stores = make(map[string]string) //genero el map vacío

// 		for message := range *msgQueue { //idealmente esto sería solo una vez para cargar las 10 stores en memoria
// 			println("[JOINER QUERY3] Leyendo stores para memoria")
// 			// jq4.logger.Debug("[Joiner Q3] Leyendo stores para memoria")
// 			packet_reader := bytes.NewReader(message.Body)
// 			packet, _ := packet.DeserializePackage(packet_reader)
// 			lines := strings.Split(packet.GetPayload(), "\n") //leo el paquete
// 			lines = lines[:len(lines)-1]

// 			for _, r := range lines { //por cada linea del paquete
// 				cols := strings.Split(r, ",")
// 				println("[JOINER QUERY3] Store ", cols[1], " para memoria")
// 				if len(cols) < 2 {
// 					panic("No hay 2 columnas como se esperaba") //store_id, store_name
// 				}
// 				worker.Stores[cols[0]] = cols[1] //lo guardo en memoria
// 			}
// 			err := message.Ack(false)
// 			if err != nil {
// 				panic(fmt.Errorf("Could not ack, %w", err))
// 			}
// 		}
// 		println("[JOINER QUERY3] Leyendo global aggregations ")
// 		colaEntradaTransactions, err := middleware.CreateQueue("GlobalAggregation3", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
// 		if err != nil {
// 			panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
// 		}
// 		msgQueueTransactions, consumeErrorT := colaEntradaTransactions.StartConsuming()
// 		if consumeErrorT != 0 {
// 			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
// 		}
// 		for message := range *msgQueueTransactions { //acá debería haber solo 1 paquete del global aggregator
// 			packetReader := bytes.NewReader(message.Body)
// 			pkt, _ := packet.DeserializePackage(packetReader)

// 			paquetesSalida := worker.Process(pkt, "query3")
// 			fmt.Printf("PAQUETES SALIDA %v\n", paquetesSalida)
// 			for _, pkt := range paquetesSalida {
// 				jq4.logger.Info("[Joiner Q3] Mando packet joineado a siguiente cola")
// 				colaSalida, err := middleware.CreateQueue("SalidaQuery4", middleware.ChannelOptionsDefault())
// 				if err != nil {
// 					panic(fmt.Errorf("CreateQueue(SalidaQuery4): %w", err))
// 				}
// 				_ = colaSalida.Send(pkt.Serialize())
// 			}

// 			err := message.Ack(false)
// 			if err != nil {
// 				panic(fmt.Errorf("Could not ack, %w", err))
// 			}
// 		}
// 	}

// }
