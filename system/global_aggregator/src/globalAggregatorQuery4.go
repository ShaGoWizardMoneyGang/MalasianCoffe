package global_aggregator

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"

	// "malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
)

type aggregator4Global struct {
	inputChannel  chan packet.Packet
	outputChannel chan packet.Packet

	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[string]map[string]uint64

	receiver packet.PacketReceiver

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator4Global) Build(rabbitAddr string) {
	g.inputChannel = make(chan packet.Packet)
	g.outputChannel = make(chan packet.Packet)

	g.colaEntrada = colas.InstanceQueue("PartialCountedUsers4", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation4", rabbitAddr)
	g.acc = make(map[string]map[string]uint64)

	g.receiver = packet.NewPacketReceiver("Aggregator 4")
	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery4, g.outputChannel)
}

func aggregateQuery4(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	localReceiver := packet.NewPacketReceiver("Agregador global 4")
	localAcc := make(map[string]map[string]uint64)

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
			userID := cols[0]
			storeID := cols[1]
			amountStr := cols[2]

			amount, err := strconv.ParseUint(amountStr, 10, 64)
			if err != nil {
				panic("Amount con formato inválido")
			}

			if localAcc[storeID] == nil {
				localAcc[storeID] = make(map[string]uint64)
			}
			localAcc[storeID][userID] += amount
		}

		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global 4")
			continue
		}

		var b strings.Builder
		stores := make([]string, 0, len(localAcc))
		for store := range localAcc {
			stores = append(stores, store)
		}
		sort.Strings(stores)

		for _, store := range stores {
			users := localAcc[store]

			type UserAmount struct {
				user   string
				amount uint64
			}

			sortedSlice := make([]UserAmount, 0, len(users))
			for user, amount := range users {
				sortedSlice = append(sortedSlice, UserAmount{user: user, amount: amount})
			}

			sort.Slice(sortedSlice, func(i, j int) bool {
				return sortedSlice[i].amount > sortedSlice[j].amount
			})

			var size int
			if len(sortedSlice) < 3 {
				size = len(sortedSlice)
			} else {
				size = 3
			}

			for i := 0; i < size; i++ {
				fmt.Fprintf(&b, "%s,%s\n", store, sortedSlice[i].user)
			}
		}

		final := b.String()
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transactions", []string{final})
			outputChannel <- newPkts[0]
		}

		localAcc = make(map[string]map[string]uint64)
		localReceiver = packet.NewPacketReceiver("Agregador global 4")
	}
}

func inputQueueQuery4(queue *middleware.MessageMiddlewareQueue, inputChannel chan<- packet.Packet) {
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

func (g *aggregator4Global) Process() {
	go inputQueueQuery4(g.colaEntrada, g.inputChannel)

	for {
		select {
		case inputPacket := <-g.inputChannel:
			g.sessionHandler.PassPacketToSession(inputPacket)
		case packetAgregado := <-g.outputChannel:
			g.colaSalida.Send(packetAgregado.Serialize())
		}
	}
}
