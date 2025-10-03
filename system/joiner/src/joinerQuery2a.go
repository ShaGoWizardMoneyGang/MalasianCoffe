package joiner

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery2a struct {
	// Tenemos una go routine por cada session
	sessions map[string](chan packet.Packet)

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2a *middleware.MessageMiddlewareQueue
}

// Devuelve true cuando quedan recibio todos los paquetes, sino false
func addMenuItemToMap(menuItemPkt packet.Packet, menuItemMap map[string]string) bool {
	stores := menuItemPkt.GetPayload()
	lines := strings.Split(stores, "\n")
	lines = lines[:len(lines)-1]
	for _, line := range lines {
		// item_id , item_name
		cols := strings.Split(line, ",")
		itemID, storeName := cols[0], cols[1]
		menuItemMap[itemID] = storeName
	}

	// TODO: verificar paquetes fuera de orden. En teoria se deberia poder
	// aislar en esta funcion o packet receiver en el directorio de packets
	// Ver: https://github.com/ShaGoWizardMoneyGang/MalasianCoffe/issues/46
	if menuItemPkt.IsEOF() {
		// Me llegaron todos
		return true
	} else {
		return false
	}
}

func joinQuery2a(inputChannel chan packet.Packet, outputQueue *middleware.MessageMiddlewareQueue) {
	// item_id -> item_name
	items := make(map[string]string)
	all_items_received := false

	all_transaction_items_received := false
	// Aca me guardo todos los packets de transaction_items que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	var transaction_items strings.Builder

	// Resultado final
	var joinedTransactionItems strings.Builder

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pkt := <-inputChannel
		fmt.Printf("Recibi %v\n", pkt)

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "menu_items" {
			all_items_received = addMenuItemToMap(pkt, items)
		} else if dataset_name == "transaction_items" {

			// Nos guardamos los que llegaron
			_, err := transaction_items.WriteString(pkt.GetPayload())
			if err != nil {
				panic("Joiner failed to add payload to transaction buffer")
			}

			// No joineamos hasta tener todos las stores

			all_transaction_items_received = pkt.IsEOF()

		} else {
			panic(fmt.Errorf("JoinerQuery2a received packet from dataset that was not expecting: %s", dataset_name))
		}

		if all_items_received {
			// WARNING: transactions queda vacio despues de esta funcion
			joinerFunctionQuery2a(&transaction_items, items, &joinedTransactionItems)

			if all_transaction_items_received {
				last_packet = pkt
				break
			}
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"menu_items", "transaction_items"}, []string{joinedTransactionItems.String()})
	// Liberamos
	joinedTransactionItems.Reset()

	slog.Info("Envio pkt joineado al sender")
	// Deberia ser 1 solo
	for _, pkt := range pkt_joineado {
		fmt.Printf("%+v\n", pkt)
		outputQueue.Send(pkt.Serialize())
	}
}

func (jq2a *joinerQuery2a) passPacketToJoiner(pkt packet.Packet) {
	slog.Info("Envio packete a la session")
	sessionID := pkt.GetSessionID()
	channel, exists := jq2a.sessions[sessionID]

	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		// Joiner
		slog.Info("Creo un hilo joiner")
		assigned_channel := make(chan packet.Packet)
		go joinQuery2a(assigned_channel, jq2a.colaSalidaQuery2a)

		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		jq2a.sessions[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pkt
}

func (jq2a *joinerQuery2a) Build(rabbitAddr string) {
	slog.Info("Inicializo el Joiner 3")
	// SessionID -> channel
	sessionHandler := make(map[string](chan packet.Packet))
	// canalSalida              := make(chan packet.Packet)

	colaSalidaQuery2a := colas.InstanceQueue("SalidaQuery2a", rabbitAddr)

	colaMenuItemsInput := colas.InstanceQueue("FilteredMenuItems2a", rabbitAddr)
	colaAggTransItems := colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)

	jq2a.sessions = sessionHandler

	jq2a.colaMenuItemsInput = colaMenuItemsInput
	jq2a.colaAggItemsInput = colaAggTransItems

	jq2a.colaSalidaQuery2a = colaSalidaQuery2a
}

func (jq2a *joinerQuery2a) send(pkt packet.Packet) {
	// SessionID -> channel
	jq2a.colaSalidaQuery2a.Send(pkt.Serialize())
}

func (jq2a *joinerQuery2a) Process() {
	slog.Info("Arranca procesamiento del joiner 2a")
	menuItemListener := make(chan packet.Packet)
	aggregatorGlobalListener := make(chan packet.Packet)

	// Stores
	go func() {
		colasEntrada := jq2a.colaMenuItemsInput

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			slog.Info("Recibi mensaje de cola de stores")
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			menuItemListener <- pkt
		}
	}()

	// Aggregated Filtered Transactions
	go func() {
		// TODO: Maybe guardar en el struct
		colasEntrada := jq2a.colaAggItemsInput

		messages := colas.ConsumeInput(colasEntrada)

		for message := range *messages {
			slog.Info("Recibi mensaje de cola de aggregated filtered transaction items")

			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			aggregatorGlobalListener <- pkt
		}
	}()

	for {
		select {
		case storePacket := <-menuItemListener:
			jq2a.passPacketToJoiner(storePacket)
		case aggregatedPacket := <-aggregatorGlobalListener:
			jq2a.passPacketToJoiner(aggregatedPacket)
			// case packetJoineado := <-jq3.canalSalida:
			// 	jq3.send(packetJoineado)
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

// // Recibe year_month_created_at, item_id, sellings_qty
// // joinea con menu_items.csv cargado en memoria con item_id, item_name
// // y me devuelve year_month_created_at, item_name, sellings_qty
func joinerFunctionQuery2a(inputTransaction *strings.Builder, menuItemMap map[string]string, joinedResult *strings.Builder) {
	input := inputTransaction.String()

	// Liberamos el buffer de input
	inputTransaction.Reset()

	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		yearMonth, itemID, sellings_qty := cols[0], cols[1], cols[2]
		itemName := menuItemMap[itemID]

		fmt.Fprintf(joinedResult, "%s,%s,%s\n", yearMonth, itemName, sellings_qty)
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
// 		output = c.joinerFunctionQuery3(input)
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
// 	case "Query3":
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
// 				colaSalida, err := middleware.CreateQueue("SalidaQuery3", middleware.ChannelOptionsDefault())
// 				if err != nil {
// 					panic(fmt.Errorf("CreateQueue(SalidaQuery3): %w", err))
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
