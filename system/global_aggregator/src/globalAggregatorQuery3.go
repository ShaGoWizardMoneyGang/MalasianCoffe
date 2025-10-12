package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
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
	inputChannel  chan packet.Packet
	outputChannel chan packet.Packet

	colaEntrada    *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator3Global) Build(rabbitAddr string, outs map[string]uint64) {
	g.inputChannel = make(chan packet.Packet)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.exchangeSalida = colas.InstanceExchange("GlobalAggregation3", rabbitAddr, outs["queue"])

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery3, g.outputChannel)
}

func aggregateQuery3(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	localReceiver := packet.NewPacketReceiver("Agregador global 3")
	localAcc := make(map[keyQuery3]float64)

	var last_packet packet.Packet

	for {
		pkt := <-inputChannel

		localReceiver.ReceivePacket(pkt)

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
			panic("Se esperaban 3 columnas")
		}
		yearHalf := cols[0]
		storeID := cols[1]
		totalStr := cols[2]

		total, err := strconv.ParseFloat(totalStr, 64)
		if err != nil {
			panic("Total con formato inválido")
		}

		k := keyQuery3{yearHalf: yearHalf, storeID: storeID}
		localAcc[k] += total
	}

	// if len(localAcc) == 0 {
	// 	localReceiver = packet.NewPacketReceiver("Agregador global 3")
	// 	continue
	// }

	keys := make([]keyQuery3, 0, len(localAcc))
	for k := range localAcc {
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
		total := localAcc[k]
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearHalf, k.storeID, total)
	}

	final := b.String()
	// if final != "" {
	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{final})
	outputChannel <- newPkts[0]
	// }
}

func (g *aggregator3Global) Process() {
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
