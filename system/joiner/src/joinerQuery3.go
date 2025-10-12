package joiner

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery3 struct {
	inputChannel chan packet.Packet

	outputChannel chan packet.Packet

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery3 *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery3(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
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

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}
}

func (jq3 *joinerQuery3) Build(rabbitAddr string, routingKey string) {
	jq3.inputChannel = make(chan packet.Packet)
	jq3.outputChannel = make(chan packet.Packet)

	jq3.colaStoresInput = colas.InstanceQueueRouted("FilteredStores3", rabbitAddr, routingKey)
	jq3.colaAggTransInput = colas.InstanceQueueRouted("GlobalAggregation3", rabbitAddr, routingKey)

	jq3.colaSalidaQuery3 = colas.InstanceQueue("SalidaQuery3", rabbitAddr)

	jq3.sessionHandler = sessionhandler.NewSessionHandler(joinQuery3, jq3.outputChannel)
}

func (jq3 *joinerQuery3) Process() {
	slog.Info("Arranca procesamiento del joiner 3")
	// Stores
	go colas.InputQueue(jq3.colaStoresInput, jq3.inputChannel)

	go colas.InputQueue(jq3.colaAggTransInput, jq3.inputChannel)

	for {
		select {
		case inputPacket := <-jq3.inputChannel:
			jq3.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq3.outputChannel:
			jq3.colaSalidaQuery3.Send(packetJoineado)
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
