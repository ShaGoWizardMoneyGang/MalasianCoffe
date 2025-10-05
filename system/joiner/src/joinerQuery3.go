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
	"malasian_coffe/system/session_handler"
)

type joinerQuery3 struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet


	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery3 *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler

}

func joinQuery3(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
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

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		switch dataset_name {
		case "stores":
			all_stores_received = addStoreToMap(pkt, storeID2Name)
		case "transactions":
			// Nos guardamos los que llegaron
			_, err := transactions.WriteString(pkt.GetPayload())
			if err != nil {
				panic("Joiner failed to add payload to transaction buffer")
			}

			all_transactions_received = pkt.IsEOF()
		default:
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

func (jq3 *joinerQuery3) Build(rabbitAddr string) {
	jq3.inputChannel      = make(chan packet.Packet)

	jq3.outputChannel     = make(chan packet.Packet)

	jq3.colaStoresInput   = colas.InstanceQueue("FilteredStores3", rabbitAddr)
	jq3.colaAggTransInput = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)

	jq3.colaSalidaQuery3  = colas.InstanceQueue("SalidaQuery3", rabbitAddr)

	jq3.sessionHandler    = sessionhandler.NewSessionHandler(joinQuery3, jq3.outputChannel)
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

	// Stores
	go inputQueue(jq3.colaStoresInput, jq3.inputChannel)

	go inputQueue(jq3.colaAggTransInput, jq3.inputChannel)

	for {
		select {
		case inputPacket := <-jq3.inputChannel:
			jq3.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq3.outputChannel:
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

