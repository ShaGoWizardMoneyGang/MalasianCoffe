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

type joinerQuery3 struct {
	// Tenemos una go routine por cada session
	sessions map[string](chan packet.Packet)

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	// TODO(fabri): puede que no haga falta
	colaSalidaQuery3 *middleware.MessageMiddlewareQueue
}

func joinQuery3(inputChannel chan packet.Packet, outputChannel chan<- packet.Packet) {
	// store_id -> store_name
	storeID2Name := make(map[string]string)
	all_stores_received := false

	// Aca me guardo todos los packets de transactions que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	var transactions strings.Builder
	all_transactions_received := false
	var joinedTransactions strings.Builder

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

		if dataset_name == "stores" {
			all_stores_received = addStoreToMap(pkt, storeID2Name)
		} else if dataset_name == "transactions" {

			// Nos guardamos los que llegaron
			_, err := transactions.WriteString(pkt.GetPayload())
			if err != nil {
				panic("Joiner failed to add payload to transaction buffer")
			}

			all_transactions_received = pkt.IsEOF()

		} else {
			panic(fmt.Errorf("JoinerQuery3 received packet from dataset that was not expecting: %s", dataset_name))
		}

		if all_stores_received {
			slog.Info("Joineo")
			// WARNING: transactions queda vacio despues de esta funcion
			joinerFunctionQuery3(&transactions, storeID2Name, &joinedTransactions)

			if all_transactions_received {
				last_packet = pkt
				break
			}
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"stores", "transactions"}, []string{joinedTransactions.String()})
	// Liberamos
	joinedTransactions.Reset()

	slog.Info("Envio pkt joineado al sender")
	// Deberia ser 1 solo
	for _, pkt := range pkt_joineado {
		fmt.Printf("%+v\n", pkt)
		outputChannel <- pkt
	}
}

func (jq3 *joinerQuery3) passPacketToJoiner(pkt packet.Packet, outputChannel chan<- packet.Packet) {
	slog.Info("Envio packete a la session")
	sessionID := pkt.GetSessionID()
	channel, exists := jq3.sessions[sessionID]

	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		// Joiner
		slog.Info("Creo un hilo joiner")
		assigned_channel := make(chan packet.Packet)
		go joinQuery3(assigned_channel, outputChannel)

		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		jq3.sessions[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pkt
}

func (jq3 *joinerQuery3) Build(rabbitAddr string) {
	slog.Info("Inicializo el Joiner 3")
	sessionHandler := make(map[string](chan packet.Packet))

	colaSalidaQuery3 := colas.InstanceQueue("SalidaQuery3", rabbitAddr)

	colaStoresInput := colas.InstanceQueue("FilteredStores3", rabbitAddr)
	colaAggTransInput := colas.InstanceQueue("GlobalAggregation3", rabbitAddr)

	jq3.sessions = sessionHandler

	jq3.colaStoresInput = colaStoresInput
	jq3.colaAggTransInput = colaAggTransInput

	jq3.colaSalidaQuery3 = colaSalidaQuery3
}

func inputQueue(input *middleware.MessageMiddlewareQueue, inputChannel chan<- packet.Packet) {
	colasEntrada := input

	messages := colas.ConsumeInput(colasEntrada)
	for message := range *messages {
		slog.Info("Recibi mensaje de cola de stores")
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}

		inputChannel <- pkt
	}
}

func (jq3 *joinerQuery3) Process() {
	slog.Info("Arranca procesamiento del joiner 3")
	inputChannel  := make(chan packet.Packet)

	outputChannel := make(chan packet.Packet)

	// Stores
	go inputQueue(jq3.colaStoresInput, inputChannel)

	go inputQueue(jq3.colaAggTransInput, inputChannel)

	for {
		select {
		case inputPacket := <-inputChannel:
			jq3.passPacketToJoiner(inputPacket, outputChannel)
		case packetJoineado := <-outputChannel:
			jq3.colaSalidaQuery3.Send(packetJoineado.Serialize())
		}
	}
}


// // Recibe year_half_created_at, store_id, tpv
// // joinea con stores.csv cargado en memoria con store_id, store_name
// // y me devuelve year_half_created_at, store_name, tpv
func joinerFunctionQuery3(inputTransaction *strings.Builder, storeMap map[string]string, joinedResult *strings.Builder) {
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
		semester, storeID, tpv := cols[0], cols[1], cols[2]
		storeName := storeMap[storeID]

		fmt.Fprintf(joinedResult, "%s,%s,%s\n", semester, storeName, tpv)
	}
}

