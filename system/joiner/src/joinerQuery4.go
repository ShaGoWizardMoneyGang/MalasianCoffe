package joiner

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

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
	colaUsersInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery4  *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}


func addUserToMap(storePkt packet.Packet, storeMap map[string]string) bool {
	stores := storePkt.GetPayload()
	lines := strings.Split(stores, "\n")
	lines = lines[:len(lines)-1]
	for _, line := range lines {
		// store_id , store_name
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeMap[store_id] = store_name
	}

	// TODO: verificar paquetes fuera de orden. En teoria se deberia poder
	// aislar en esta funcion o packet receiver en el directorio de packets
	// Ver: https://github.com/ShaGoWizardMoneyGang/MalasianCoffe/issues/46
	if storePkt.IsEOF() {
		// Me llegaron todos
		return true
	} else {
		return false
	}
}

func joinQuery4(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	// store_id -> store_name
	storeID2Name        := make(map[string]string)
	all_stores_received := false

	// store_id -> store_name
	userID2Birthday     := make(map[string]string)
	all_users_received  := false

	all_transactions_received := false
	// Aca me guardo todos los packets de transactions que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	var transactions strings.Builder

	// Resultado final
	var joinedTransactions strings.Builder

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pkt :=  <- inputChannel
		// println(pkt.GetPayload())

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		switch dataset_name {
		case "stores":
			all_stores_received = addStoreToMap(pkt, storeID2Name)
		case "users":
			all_users_received = addUserToMap(pkt, userID2Birthday)
		case "transactions":
			// Nos guardamos los que llegaron
			_, err := transactions.WriteString(pkt.GetPayload())
			if err != nil {
				panic("Joiner failed to add payload to transaction buffer")
			}

			all_transactions_received = pkt.IsEOF()
		default:
			panic(fmt.Errorf("JoinerQuery4 received packet from dataset that was not expecting: %s", dataset_name))
		}

		// Casos posibles:
		// 1:
		//   Recibi todos los transactions antes que stores y users.
		//   Apenas reciba los stores y users que me faltan, hago el join y
		//   termina el loop al toque.
		// 2:
		//   Recibi todas las stores y users antes que transactions.
		//   Voy a ir recibiendo transacciones de a poquito y las voy a ir
		//   anadiendo en el join.  Cuando llegue la ultima, esto va a cortar,
		//   y el join va a tener el resultado final en joined transactions.

		// No joineamos hasta tener todos las stores
		if all_stores_received == true && all_users_received == true {
			slog.Info("Comienza proceso de join")
			// WARNING: transactions queda vacio despues de esta funcion
			joinerFunctionQuery4(&transactions, storeID2Name, userID2Birthday, &joinedTransactions)

			if all_transactions_received {
				last_packet = pkt
				break
			}
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"users", "stores", "transactions"}, []string{joinedTransactions.String()})

	// Liberamos
	joinedTransactions.Reset()

	slog.Info("Envio pkt joineado al sender")
	// Deberia ser 1 solo
	for _, pkt := range pkt_joineado {
		fmt.Printf("%+v\n", pkt)
		outputChannel <- pkt
	}
}

// func (jq4 *joinerQuery4) passPacketToJoiner(pkt packet.Packet) {
// 	slog.Info("Envio packete a la session")
// 	sessionID := pkt.GetSessionID()
// 	channel, exists := jq4.sessions[sessionID]


// 	// Nos creamos una rutina por cada session. Nosotros le enviamos los
// 	// paquetes correspondientes a cada rutina
// 	if !exists {
// 		// Joiner
// 		slog.Info("Creo un hilo joiner")
// 		assigned_channel := make(chan packet.Packet)
// 		go joinQuery4(assigned_channel, jq4.colaSalidaQuery4)

// 		// No hace falta un mutex porque este diccionario se accede de forma
// 		// secuencial
// 		jq4.sessions[sessionID] = assigned_channel

// 		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
// 		channel = assigned_channel
// 	}

// 	channel <- pkt
// }

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
	go inputQueue(jq4.colaStoresInput, jq4.inputChannel)

	go inputQueue(jq4.colaUsersInput, jq4.inputChannel)

	go inputQueue(jq4.colaAggTransInput, jq4.inputChannel)

	for {
		select {
		case inputPacket := <-jq4.inputChannel:
			jq4.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq4.outputChannel:
			jq4.colaSalidaQuery4.Send(packetJoineado.Serialize())
		}
	}
}

// package main

// import (
// 	"bytes"
// 	"fmt"
// 	"log/slog"
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
func joinerFunctionQuery4(inputTransaction *strings.Builder, storeMap map[string]string, userMap map[string]string, joinedResult *strings.Builder) {
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
// 			// slog.Debug("[Joiner Q3] Leyendo stores para memoria")
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
// 				slog.Info("[Joiner Q3] Mando packet joineado a siguiente cola")
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
