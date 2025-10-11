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

type joinerQuery2a struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2a *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery2a(inputChannel  <-chan packet.Packet, outputChannel chan<- packet.Packet) {
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

func (jq2a *joinerQuery2a) Build(rabbitAddr string) {
	jq2a.inputChannel       = make(chan packet.Packet)
	jq2a.outputChannel      = make(chan packet.Packet)

	jq2a.colaMenuItemsInput = colas.InstanceQueue("FilteredMenuItems2a", rabbitAddr)
	jq2a.colaAggItemsInput  = colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)

	jq2a.colaSalidaQuery2a  = colas.InstanceQueue("SalidaQuery2a", rabbitAddr)

	jq2a.sessionHandler     = sessionhandler.NewSessionHandler(joinQuery2a, jq2a.outputChannel)
}

func (jq2a *joinerQuery2a) Process() {
	slog.Info("Arranca procesamiento del joiner 2a")

	go inputQueue(jq2a.colaMenuItemsInput, jq2a.inputChannel)

	go inputQueue(jq2a.colaAggItemsInput, jq2a.inputChannel)

	for {
		select {
		case inputPacket := <-jq2a.inputChannel:
			jq2a.sessionHandler.PassPacketToSession(inputPacket)
		case aggregatedPacket := <-jq2a.outputChannel:
			jq2a.colaSalidaQuery2a.Send(aggregatedPacket.Serialize())
		}
	}
}

func joinerFunctionQuery2a(menuItemReceiver packet.PacketReceiver, transactionItemReceiver packet.PacketReceiver, joinedTransactionItems *strings.Builder) {
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
