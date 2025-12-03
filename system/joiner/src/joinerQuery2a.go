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

type joinerQuery2a struct {
	inputChannel chan colas.PacketMessage

	outputChannel chan packet.Packet

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2a *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery2a(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	expected_datasets := []multiple_packet_receiver.NombreDataset {
		multiple_packet_receiver.NombreDataset("menu_items"),
		multiple_packet_receiver.NombreDataset("transaction_items"),
	};

	packet_receiver := multiple_packet_receiver.NewMultiplePacketReceiver(sessionID, expected_datasets, joinerFunctionQuery2a)

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

func (jq2a *joinerQuery2a) Build(rabbitAddr string, routingKey string) {
	jq2a.inputChannel = make(chan colas.PacketMessage)
	jq2a.outputChannel = make(chan packet.Packet)

	jq2a.colaMenuItemsInput = colas.InstanceQueueRouted("FilteredMenuItems2a", rabbitAddr, routingKey)
	jq2a.colaAggItemsInput = colas.InstanceQueueRouted("GlobalAggregation2a", rabbitAddr, routingKey)

	jq2a.colaSalidaQuery2a = colas.InstanceQueue("SalidaQuery2a", rabbitAddr)

	jq2a.sessionHandler = sessionhandler.NewSessionHandler(joinQuery2a, jq2a.outputChannel)
}

func (jq2a *joinerQuery2a) Process() {
	slog.Info("Arranca procesamiento del joiner 2a")

	go colas.InputQueue(jq2a.colaMenuItemsInput, jq2a.inputChannel)

	go colas.InputQueue(jq2a.colaAggItemsInput, jq2a.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-jq2a.inputChannel:
			jq2a.sessionHandler.PassPacketToSession(inputPacket)
		case aggregatedPacket := <-jq2a.outputChannel:
			jq2a.colaSalidaQuery2a.Send(aggregatedPacket)
			ackPkt := colas.NewAnswerPacket(aggregatedPacket)
			jq2a.sessionHandler.PassPacketToSession(ackPkt)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Joiner Query2a received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}

func joinerFunctionQuery2a(inputs map[multiple_packet_receiver.NombreDataset]multiple_packet_receiver.ContenidoCompleto) string {
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

