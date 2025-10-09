package joiner

import (
	"bytes"

	"strconv"
	"strings"
	"fmt"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery4 struct {
	// Tenemos una go routine por cada session
	sessions map[string](chan packet.Packet)

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaUsersInput    *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery4 *middleware.MessageMiddlewareQueue
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

func joinQuery4(inputChannel chan packet.Packet, outputQueue *middleware.MessageMiddlewareQueue) {
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
		bitacora.Info(fmt.Sprintf("Envio paquete joineado de ID %v, al sender", pkt.GetUUID()))
		outputQueue.Send(pkt.Serialize())
	}
}

func (jq4 *joinerQuery4) passPacketToJoiner(pkt packet.Packet) {
	sessionID := pkt.GetSessionID()
	channel, exists := jq4.sessions[sessionID]

	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		// Joiner
		bitacora.Info("Creo un hilo joiner")
		assigned_channel := make(chan packet.Packet)
		go joinQuery4(assigned_channel, jq4.colaSalidaQuery4)

		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		jq4.sessions[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pkt
}

func (jq4 *joinerQuery4) Build(rabbitAddr string) {
	// SessionID -> channel
	sessionHandler := make(map[string](chan packet.Packet))

	colaSalidaQuery4 := colas.InstanceQueue("SalidaQuery4", rabbitAddr)

	colaStoresInput := colas.InstanceQueue("FilteredStores4", rabbitAddr)
	colaUsersInput := colas.InstanceQueue("FilteredUsers4", rabbitAddr)
	colaAggTransInput := colas.InstanceQueue("GlobalAggregation4", rabbitAddr)

	jq4.sessions = sessionHandler

	jq4.colaStoresInput = colaStoresInput
	jq4.colaUsersInput = colaUsersInput
	jq4.colaAggTransInput = colaAggTransInput

	jq4.colaSalidaQuery4 = colaSalidaQuery4
}

func (jq4 *joinerQuery4) Process() {
	bitacora.Info("Arranca procesamiento del joiner 4")
	storeListener := make(chan packet.Packet)
	userListener := make(chan packet.Packet)
	aggregatorGlobalListener := make(chan packet.Packet)

	// Stores
	go func() {
		colasEntrada := jq4.colaStoresInput

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				bitacora.Error(fmt.Errorf("Falle al tratar de enviar un ack al pkt de ID %s, con error: %w", pkt.GetUUID(), err).Error())
			}

			storeListener <- pkt
		}
	}()

	// Users
	go func() {
		colasEntrada := jq4.colaUsersInput

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				bitacora.Error(fmt.Errorf("Falle al tratar de enviar un ack al pkt de ID %s, con error: %w", pkt.GetUUID(), err).Error())
			}

			userListener <- pkt
		}
	}()

	// Aggregated Filtered Transactions
	go func() {
		// TODO: Maybe guardar en el struct
		colasEntrada := jq4.colaAggTransInput

		messages := colas.ConsumeInput(colasEntrada)

		for message := range *messages {

			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				bitacora.Error(fmt.Errorf("Falle al tratar de enviar un ack al pkt de ID %s, con error: %w", pkt.GetUUID(), err).Error())
			}

			aggregatorGlobalListener <- pkt
		}
	}()

	for {
		select {
		case storePacket := <-storeListener:
			jq4.passPacketToJoiner(storePacket)
		case aggregatedPacket := <-aggregatorGlobalListener:
			jq4.passPacketToJoiner(aggregatedPacket)
		case userPacket := <-userListener:
			jq4.passPacketToJoiner(userPacket)
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
