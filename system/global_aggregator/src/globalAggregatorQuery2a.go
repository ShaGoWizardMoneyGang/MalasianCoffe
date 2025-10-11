package global_aggregator

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
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
	inputChannel  chan packet.Packet
	outputChannel chan packet.Packet

	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue

	receiver packet.PacketReceiver

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator2aGlobal) Build(rabbitAddr string) {
	g.inputChannel = make(chan packet.Packet)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueue("CountedItems2a", rabbitAddr)
	// aca va GlobalAggregation2a
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)

	g.receiver = packet.NewPacketReceiver("Agregador global 2a")

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery2a, g.outputChannel)
}

func aggregateQuery2a(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	localReceiver := packet.NewPacketReceiver("Agregador global 2a")
	localAcc := make(map[keyQuery2a]int64)

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

		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global 2a")
			continue
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
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transaction_items", []string{final})
			outputChannel <- newPkts[0]
		}

		localAcc = make(map[keyQuery2a]int64)
		localReceiver = packet.NewPacketReceiver("Agregador global 2a")
	}
}

func inputQueueQuery2a(queue *middleware.MessageMiddlewareQueue, inputChannel chan<- packet.Packet) {
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

func (g *aggregator2aGlobal) Process() {
	go inputQueueQuery2a(g.colaEntrada, g.inputChannel)

	for {
		select {
		case inputPacket := <-g.inputChannel:
			g.sessionHandler.PassPacketToSession(inputPacket)
		case packetAgregado := <-g.outputChannel:
			g.colaSalida.Send(packetAgregado.Serialize())
		}
	}
}
