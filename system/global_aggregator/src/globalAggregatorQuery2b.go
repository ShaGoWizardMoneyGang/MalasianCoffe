package global_aggregator

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"sort"
	"strconv"
	"strings"
)

// AggregatorGLobal de la query2b ES LA SUBTOTAL
type keyQuery2b struct {
	yearMonth string
	itemID    string
}

type aggregator2bGlobal struct {
	inputChannel  chan colas.PacketMessage
	outputChannel chan packet.Packet

	colaEntrada    *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator2bGlobal) Build(rabbitAddr string, routing_key string, outs map[string]uint64) {
	g.inputChannel = make(chan colas.PacketMessage)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueueRouted("CountedItems2b", rabbitAddr, routing_key)
	// aca va GlobalAggregation2b
	g.exchangeSalida = colas.InstanceExchange("GlobalAggregation2b", rabbitAddr, outs["queue"])

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateSessionQuery2b, g.outputChannel)
}

func aggregateSessionQuery2b(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := packet_receiver.NewPacketReceiver("agregador-global-2b")
	localAcc := make(map[keyQuery2b]float64)

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet
	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		localReceiver.ReceivePacket(pktMsg)

		if localReceiver.ReceivedAll() {
			last_packet = pkt
			break
		}
	}

	consolidatedInput := localReceiver.GetPayload()

	lines := strings.Split(consolidatedInput, "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			bitacora.Debug("Se esperaban 3 columnas")
			continue
		}
		yearMonth := cols[0]
		itemID := cols[1]
		subtotalStr := cols[2]

		subtotal, err := strconv.ParseFloat(subtotalStr, 64)
		if err != nil {
			panic("Subtotal con formato inválido")
		}

		k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
		localAcc[k] += subtotal
	}

	monthlyMax := make(map[string]struct {
		itemID   string
		subtotal float64
	})

	for k, value := range localAcc {
		yearMonth := k.yearMonth
		if current, exists := monthlyMax[yearMonth]; !exists || value > current.subtotal {
			monthlyMax[yearMonth] = struct {
				itemID   string
				subtotal float64
			}{
				itemID:   k.itemID,
				subtotal: value,
			}
		}
	}

	var b strings.Builder
	months := make([]string, 0, len(monthlyMax))
	for month := range monthlyMax {
		months = append(months, month)
	}
	sort.Strings(months)

	for _, month := range months {
		maxItem := monthlyMax[month]
		fmt.Fprintf(&b, "%s,%s,%.2f\n", month, maxItem.itemID, maxItem.subtotal)
	}

	final := b.String()
	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transaction_items", []string{final})
	outputChannel <- newPkts[0]
}

func (g *aggregator2bGlobal) Process() {

	go colas.InputQueue(g.colaEntrada, g.inputChannel)

	for {
		select {
		case inputPacket := <-g.inputChannel:
			g.sessionHandler.PassPacketToSession(inputPacket)
		case packetAgregado := <-g.outputChannel:
			g.exchangeSalida.Send(packetAgregado)
		}
	}
}
