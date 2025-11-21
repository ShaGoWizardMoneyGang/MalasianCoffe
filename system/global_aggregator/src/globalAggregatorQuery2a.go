package global_aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
)

// AggregatorGLobal de la query2a ES LA QUANTITY
type keyQuery2a struct {
	yearMonth string
	itemID    string
}

type aggregator2aGlobal struct {
	inputChannel  chan colas.PacketMessage
	outputChannel chan packet.Packet

	colaEntrada *middleware.MessageMiddlewareQueue
	//colaSalida  *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator2aGlobal) Build(rabbitAddr string, routing_key string, outs map[string]uint64) {
	g.inputChannel = make(chan colas.PacketMessage)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueueRouted("CountedItems2a", rabbitAddr, routing_key)
	// aca va GlobalAggregation2a
	g.exchangeSalida = colas.InstanceExchange("GlobalAggregation2a", rabbitAddr, outs["queue"])

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery2a, g.outputChannel)
}

func aggregateQuery2a(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := packet_receiver.NewPacketReceiver("agregador-global-2a")
	localAcc := make(map[keyQuery2a]int64)

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
		quantityStr := cols[2]

		quantity, err := strconv.ParseInt(quantityStr, 10, 64)
		if err != nil {
			panic("Quantity con formato inválido")
		}

		k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
		localAcc[k] += quantity
	}

	monthlyMax := make(map[string]struct {
		itemID   string
		quantity int64
	})

	for k, value := range localAcc {
		yearMonth := k.yearMonth
		if current, exists := monthlyMax[yearMonth]; !exists || value > current.quantity {
			monthlyMax[yearMonth] = struct {
				itemID   string
				quantity int64
			}{
				itemID:   k.itemID,
				quantity: value,
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
		fmt.Fprintf(&b, "%s,%s,%d\n", month, maxItem.itemID, maxItem.quantity)
	}

	final := b.String()

	// QUESTION: Ojo, es *RE* importante que siempre mandemos un paquete. Si
	// por algun motivo esto esta vacio, hay que enviar el texto vacio porque
	// sino rompemos la invariante del sistema del ordenamiento de paquetes.
	// if final != "" {
	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transaction_items", []string{final})

	outputChannel <- newPkts[0]
	// }
}

func (g *aggregator2aGlobal) Process() {
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
