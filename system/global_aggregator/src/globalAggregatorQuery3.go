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

type keyQuery3 struct {
	yearHalf string // "YYYY-H1" o "YYYY-H2"
	storeID  string
}

type aggregator3Global struct {
	inputChannel  chan colas.PacketMessage
	outputChannel chan packet.Packet

	colaEntrada    *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator3Global) Build(rabbitAddr string, routing_key string, outs map[string]uint64) {
	// fmt.Printf("OUTS: %v\n", outs)
	g.inputChannel = make(chan colas.PacketMessage)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueueRouted("PartialAggregations3", rabbitAddr, routing_key)
	g.exchangeSalida = colas.InstanceExchange("GlobalAggregation3", rabbitAddr, outs["queue"])

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery3, g.outputChannel)
}

func aggregate_3_func(accumulated_input string, new_input string) string {
	localAcc := make(map[keyQuery3]float64)
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
			yearHalf := columns[0]
			storeID := columns[1]
			totalStr := columns[2]

			total, err := strconv.ParseFloat(totalStr, 64)
			if err != nil {
				panic("Total con formato inválido")
			}

			k := keyQuery3{yearHalf: yearHalf, storeID: storeID}
			localAcc[k] = total
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
		yearHalf := columns[0]
		storeID := columns[1]
		totalStr := columns[2]

		total, err := strconv.ParseFloat(totalStr, 64)
		if err != nil {
			panic("Total con formato inválido")
		}

		k := keyQuery3{yearHalf: yearHalf, storeID: storeID}
		localAcc[k] += total
	}

	var b strings.Builder
	for key, total := range localAcc {
		fmt.Fprintf(&b, "%s,%s,%f\n", key.yearHalf, key.storeID, total)
	}

	return b.String()
}

func aggregateQuery3(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := single_packet_receiver.NewSinglePacketReceiver(sessionID, aggregate_3_func)

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

	totalAcc := make(map[keyQuery3]float64)

	lines := strings.Split(aggregated_packets, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		columns := strings.Split(line, ",")
		if len(columns) != 3 {
			continue
		}
		yearHalf := columns[0]
		storeID := columns[1]
		totalStr := columns[2]

		total, err := strconv.ParseFloat(totalStr, 64)
		if err != nil {
			panic("Total con formato inválido")
		}

		k := keyQuery3{yearHalf: yearHalf, storeID: storeID}
		totalAcc[k] = total
	}

	keys := make([]keyQuery3, 0, len(totalAcc))
	for k := range totalAcc {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].yearHalf == keys[j].yearHalf {
			return keys[i].storeID < keys[j].storeID
		}
		return keys[i].yearHalf < keys[j].yearHalf
	})

	var b strings.Builder
	for _, k := range keys {
		total := totalAcc[k]
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearHalf, k.storeID, total)
	}

	final := b.String()

	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{final})

	outputChannel <- newPkts[0]

	colas.WaitForAnswer(inputChannel)
	localReceiver.Clean()

}

func (g *aggregator3Global) Process() {
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
