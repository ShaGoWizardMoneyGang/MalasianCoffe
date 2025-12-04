package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/single_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"
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

func aggregate_2b_func(accumulated_input string, new_input string) string {
	localAcc := make(map[keyQuery2b]float64)

	if accumulated_input != "" {
		lines := strings.Split(accumulated_input, "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			columns := strings.Split(line, ",")
			if len(columns) != 3 {
				continue
			}
			yearMonth := columns[0]
			itemID := columns[1]
			subtotalStr := columns[2]

			subtotal, err := strconv.ParseFloat(subtotalStr, 64)
			if err != nil {
				panic("Subtotal con formato inválido")
			}

			k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
			localAcc[k] = subtotal
		}
	}

	lines := strings.Split(new_input, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		columns := strings.Split(line, ",")
		if len(columns) != 3 {
			continue
		}
		yearMonth := columns[0]
		itemID := columns[1]
		subtotalStr := columns[2]

		subtotal, err := strconv.ParseFloat(subtotalStr, 64)
		if err != nil {
			panic("Subtotal con formato inválido")
		}

		k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
		localAcc[k] += subtotal

	}

	var b strings.Builder
	for key, subtotal := range localAcc {
		fmt.Fprintf(&b, "%s,%s,%f\n", key.yearMonth, key.itemID, subtotal)
	}

	return b.String()
}

func aggregateSessionQuery2b(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := single_packet_receiver.NewSinglePacketReceiver(sessionID, aggregate_2b_func)

	var last_packet packet.Packet
	var aggregated_packets string

	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		received_all := localReceiver.ReceivePacket(pktMsg)

		if !received_all {
			continue
		}

		aggregated_packets = localReceiver.GetPayload()
		last_packet = pkt
		break
	}

	totalAcc := make(map[keyQuery2b]float64)

	lines := strings.Split(aggregated_packets, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		columns := strings.Split(line, ",")
		if len(columns) != 3 {
			continue
		}
		yearMonth := columns[0]
		itemID := columns[1]
		subtotalStr := columns[2]

		subtotal, err := strconv.ParseFloat(subtotalStr, 64)
		if err != nil {
			panic("Subtotal con formato inválido")
		}

		k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
		totalAcc[k] = subtotal
	}

	monthlyMax := make(map[string]struct {
		itemID   string
		subtotal float64
	})

	for k, value := range totalAcc {
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

	var topSubtotalProducts strings.Builder
	months := make([]string, 0, len(monthlyMax))
	for month := range monthlyMax {
		months = append(months, month)
	}
	sort.Strings(months)

	for _, month := range months {
		maxItem := monthlyMax[month]
		fmt.Fprintf(&topSubtotalProducts, "%s,%s,%.2f\n", month, maxItem.itemID, maxItem.subtotal)
	}

	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transaction_items", []string{topSubtotalProducts.String()})

	outputChannel <- newPkts[0]

	colas.WaitForAnswer(inputChannel)
	localReceiver.Clean()
}

func (g *aggregator2bGlobal) Process() {

	go colas.InputQueue(g.colaEntrada, g.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-g.inputChannel:
			g.sessionHandler.PassPacketToSession(inputPacket)
		case packetAgregado := <-g.outputChannel:
			g.exchangeSalida.Send(packetAgregado)
			ackPkt := colas.NewAnswerPacket(packetAgregado)
			g.sessionHandler.PassPacketToSession(ackPkt)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			watchdog.Pong(IP)
		}
	}
}
