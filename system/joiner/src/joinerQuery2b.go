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

type joinerQuery2b struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2b *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery2b(inputChannel  <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	menuItemReceiver := packet.NewPacketReceiver("Menu items")

	transactionItemReceiver := packet.NewPacketReceiver("Transaction items")

	var joinedTransactionItems strings.Builder

	var last_packet packet.Packet

	for {
		pkt := <-inputChannel

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "menu_items" {
			menuItemReceiver.ReceivePacket(pkt)
		} else if dataset_name == "transaction_items" {
			transactionItemReceiver.ReceivePacket(pkt)
		} else {
			panic(fmt.Errorf("JoinerQuery2b received packet from dataset that was not expecting: %s", dataset_name))
		}

		if menuItemReceiver.ReceivedAll() && transactionItemReceiver.ReceivedAll() {
			slog.Info("Comienza proceso de join")
			joinerFunctionQuery2b(menuItemReceiver, transactionItemReceiver, &joinedTransactionItems)

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

func (jq2b *joinerQuery2b) Build(rabbitAddr string, routingKey string) {
	jq2b.inputChannel       = make(chan packet.Packet)
	jq2b.outputChannel      = make(chan packet.Packet)

	jq2b.colaMenuItemsInput = colas.InstanceQueue("FilteredMenuItems2b", rabbitAddr)
	jq2b.colaAggItemsInput  = colas.InstanceQueue("GlobalAggregation2b", rabbitAddr)

	jq2b.colaSalidaQuery2b  = colas.InstanceQueue("SalidaQuery2b", rabbitAddr)

	jq2b.sessionHandler     = sessionhandler.NewSessionHandler(joinQuery2b, jq2b.outputChannel)

}

func (jq2b *joinerQuery2b) Process() {
	slog.Info("Arranca procesamiento del joiner 2b")

	go colas.InputQueue(jq2b.colaMenuItemsInput, jq2b.inputChannel)

	go colas.InputQueue(jq2b.colaAggItemsInput, jq2b.inputChannel)

	for {
		select {
		case inputPacket := <-jq2b.inputChannel:
			jq2b.sessionHandler.PassPacketToSession(inputPacket)
		case aggregatedPacket := <-jq2b.outputChannel:
			jq2b.colaSalidaQuery2b.Send(aggregatedPacket)
		}
	}
}

func joinerFunctionQuery2b(menuItemReceiver packet.PacketReceiver, transactionItemReceiver packet.PacketReceiver, joinedTransactionItems *strings.Builder) {
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
