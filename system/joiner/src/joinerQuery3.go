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

	colaSalidaQuery3 *middleware.MessageMiddlewareQueue
}

func joinQuery3(inputChannel chan packet.Packet, outputQueue *middleware.MessageMiddlewareQueue) {
	storeReceiver := packet.NewPacketReceiver("Stores")

	transactionReceiver := packet.NewPacketReceiver("Transactions")

	var joinedTransactions strings.Builder

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
		} else if dataset_name == "transactions" {
			transactionReceiver.ReceivePacket(pkt)
		} else {
			panic(fmt.Errorf("JoinerQuery3 received packet from dataset that was not expecting: %s", dataset_name))
		}

		if storeReceiver.ReceivedAll() && transactionReceiver.ReceivedAll() {
			slog.Info("Comienza proceso de join")
			joinerFunctionQuery3(storeReceiver, transactionReceiver, &joinedTransactions)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"stores", "transactions"}, []string{joinedTransactions.String()})

	joinedTransactions.Reset()

	slog.Info("Envio pkt joineado al sender")
	for _, pkt := range pkt_joineado {
		outputQueue.Send(pkt.Serialize())
	}
}

func (jq3 *joinerQuery3) passPacketToJoiner(pkt packet.Packet) {
	slog.Info("Envio packete a la session")
	sessionID := pkt.GetSessionID()
	channel, exists := jq3.sessions[sessionID]

	if !exists {
		slog.Info("Creo un hilo joiner")
		assigned_channel := make(chan packet.Packet)
		go joinQuery3(assigned_channel, jq3.colaSalidaQuery3)

		jq3.sessions[sessionID] = assigned_channel

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

func (jq3 *joinerQuery3) Process() {
	slog.Info("Arranca procesamiento del joiner 3")
	storeListener := make(chan packet.Packet)
	aggregatorGlobalListener := make(chan packet.Packet)

	go func() {
		colasEntrada := jq3.colaStoresInput

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			storeListener <- pkt
		}
	}()

	go func() {
		colasEntrada := jq3.colaAggTransInput

		messages := colas.ConsumeInput(colasEntrada)

		for message := range *messages {
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
		case storePacket := <-storeListener:
			jq3.passPacketToJoiner(storePacket)
		case aggregatedPacket := <-aggregatorGlobalListener:
			jq3.passPacketToJoiner(aggregatedPacket)
		}
	}
}

func joinerFunctionQuery3(storeReceiver packet.PacketReceiver, transactionReceiver packet.PacketReceiver, joinedTransactions *strings.Builder) {
	storeMap := createStoreMap(storeReceiver)

	transactions := transactionReceiver.GetPayload()
	lines := strings.Split(transactions, "\n")
	lines = lines[:len(lines)-1]

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		semester, storeID, tpv := cols[0], cols[1], cols[2]
		storeName := storeMap[storeID]

		fmt.Fprintf(joinedTransactions, "%s,%s,%s\n", semester, storeName, tpv)
	}
}
