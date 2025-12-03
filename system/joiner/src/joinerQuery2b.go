package joiner

import (
	"fmt"
	"log/slog"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/multiple_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
)

type joinerQuery2b struct {
	inputChannel chan colas.PacketMessage

	outputChannel chan packet.Packet

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2b *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery2b(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	expected_datasets := []multiple_packet_receiver.NombreDataset {
		multiple_packet_receiver.NombreDataset("menu_items"),
		multiple_packet_receiver.NombreDataset("transaction_items"),
	};

	packet_receiver := multiple_packet_receiver.NewMultiplePacketReceiver(sessionID, expected_datasets, joinerFunctionQuery2b)

	var joinedTransactions string

	var last_packet packet.Packet

	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		received_all := packet_receiver.ReceivePacket(pktMsg)

		if !received_all {
			continue
		}

		joinedTransactions = packet_receiver.GetPayload()
		last_packet = pkt
		break
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"menu_items", "transaction_items"}, []string{joinedTransactions})

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}

	colas.WaitForAnswer(inputChannel)
	packet_receiver.Clean()
}

func (jq2b *joinerQuery2b) Build(rabbitAddr string, routingKey string) {
	jq2b.inputChannel = make(chan colas.PacketMessage)
	jq2b.outputChannel = make(chan packet.Packet)

	jq2b.colaMenuItemsInput = colas.InstanceQueueRouted("FilteredMenuItems2b", rabbitAddr, routingKey)
	jq2b.colaAggItemsInput = colas.InstanceQueueRouted("GlobalAggregation2b", rabbitAddr, routingKey)

	jq2b.colaSalidaQuery2b = colas.InstanceQueue("SalidaQuery2b", rabbitAddr)

	jq2b.sessionHandler = sessionhandler.NewSessionHandler(joinQuery2b, jq2b.outputChannel)

}

func (jq2b *joinerQuery2b) Process() {
	slog.Info("Arranca procesamiento del joiner 2b")

	go colas.InputQueue(jq2b.colaMenuItemsInput, jq2b.inputChannel)

	go colas.InputQueue(jq2b.colaAggItemsInput, jq2b.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-jq2b.inputChannel:
			jq2b.sessionHandler.PassPacketToSession(inputPacket)
		case aggregatedPacket := <-jq2b.outputChannel:
			jq2b.colaSalidaQuery2b.Send(aggregatedPacket)
			ackPkt := colas.NewAnswerPacket(aggregatedPacket)
			jq2b.sessionHandler.PassPacketToSession(ackPkt)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			watchdog.Pong(IP)
		}
	}
}

func joinerFunctionQuery2b(inputs map[multiple_packet_receiver.NombreDataset]multiple_packet_receiver.ContenidoCompleto) string {
	menuItemMap := createMenuItemMap(string(inputs["menu_items"]))
	transactionItems := string(inputs["transaction_items"])

	var joinedTransaction strings.Builder

	lines := strings.Split(transactionItems, "\n")
	lines = lines[:len(lines)-1]

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		yearMonth, itemID, sellings_qty := cols[0], cols[1], cols[2]
		itemName := menuItemMap[itemID]

		fmt.Fprintf(&joinedTransaction, "%s,%s,%s\n", yearMonth, itemName, sellings_qty)
	}

	final_result := joinedTransaction.String()

	return final_result
}
