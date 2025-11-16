package joiner

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
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

func joinQuery2a(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	menuItemReceiver := packet_receiver.NewSinglePacketReceiver("menu-items")

	transactionItemReceiver := packet_receiver.NewSinglePacketReceiver("transaction-items")

	var joinedTransactionItems strings.Builder

	var last_packet packet.Packet

	for {
		pktMsg := <-inputChannel
		pkt    := pktMsg.Packet

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "menu_items" {
			menuItemReceiver.ReceivePacket(pktMsg)
		} else if dataset_name == "transaction_items" {
			transactionItemReceiver.ReceivePacket(pktMsg)
		} else {
			panic(fmt.Errorf("JoinerQuery2a received packet from dataset that was not expecting: %s", dataset_name))
		}

		if menuItemReceiver.ReceivedAll() && transactionItemReceiver.ReceivedAll() {
			slog.Info("Comienza proceso de join")
			joinerFunctionQuery2a(menuItemReceiver, transactionItemReceiver, &joinedTransactionItems)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"menu_items", "transaction_items"}, []string{joinedTransactionItems.String()})

	joinedTransactionItems.Reset()

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}
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

	for {
		select {
		case inputPacket := <-jq2a.inputChannel:
			jq2a.sessionHandler.PassPacketToSession(inputPacket)
		case aggregatedPacket := <-jq2a.outputChannel:
			jq2a.colaSalidaQuery2a.Send(aggregatedPacket)
		}
	}
}

func joinerFunctionQuery2a(menuItemReceiver packet_receiver.SinglePacketReceiver, transactionItemReceiver packet_receiver.SinglePacketReceiver, joinedTransactionItems *strings.Builder) {
	menuItemMap := createMenuItemMap(menuItemReceiver)

	transactionItems := transactionItemReceiver.GetPayload()
	lines := strings.Split(transactionItems, "\n")
	lines = lines[:len(lines)-1]

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		yearMonth, itemID, sellings_qty := cols[0], cols[1], cols[2]
		itemName := menuItemMap[itemID]

		fmt.Fprintf(joinedTransactionItems, "%s,%s,%s\n", yearMonth, itemName, sellings_qty)
	}
}
