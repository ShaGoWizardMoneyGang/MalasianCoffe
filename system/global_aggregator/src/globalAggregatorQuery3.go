package global_aggregator

import (
	"bytes"
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

	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue

	receiver       packet.PacketReceiver
	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator3Global) Build(rabbitAddr string) {
	g.inputChannel = make(chan packet.Packet)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)

	g.receiver = packet.NewPacketReceiver("Aggregator 3")

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery3, g.outputChannel)
}

func aggregateQuery3(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	localReceiver := packet.NewPacketReceiver("Agregador global 3")
	localAcc := make(map[keyQuery3]float64)

	for {
		pkt := <-inputChannel

		localReceiver.ReceivePacket(pkt)

		if !localReceiver.ReceivedAll() {
			continue
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

		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global 3")
			continue
		}

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
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transactions", []string{final})
			outputChannel <- newPkts[0]
		}

		localAcc = make(map[keyQuery3]float64)
		localReceiver = packet.NewPacketReceiver("Agregador global 3")
	}
}

func inputQueueQuery3(queue *middleware.MessageMiddlewareQueue, inputChannel chan<- packet.Packet) {
	msgQueue := colas.ConsumeInput(queue)
	for message := range *msgQueue {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}

		inputChannel <- pkt
	}
}

func (g *aggregator3Global) Process() {
	go inputQueueQuery3(g.colaEntrada, g.inputChannel)

	for {
		select {
		case inputPacket := <-g.inputChannel:
			g.sessionHandler.PassPacketToSession(inputPacket)
		case packetAgregado := <-g.outputChannel:
			g.colaSalida.Send(packetAgregado.Serialize())
		}
	}
}
