package global_aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/single_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"

	// "malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
)

type aggregator4Global struct {
	inputChannel  chan colas.PacketMessage
	outputChannel chan packet.Packet

	colaEntrada    *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange

	sessionHandler sessionhandler.SessionHandler
}

func (g *aggregator4Global) Build(rabbitAddr string, routing_key string, outs map[string]uint64) {
	g.inputChannel = make(chan colas.PacketMessage)
	g.outputChannel = make(chan packet.Packet)

	fmt.Printf("ROUTING KEY %s\n", routing_key)
	g.colaEntrada = colas.InstanceQueueRouted("PartialCountedUsers4", rabbitAddr, routing_key)

	g.exchangeSalida = colas.InstanceExchange("GlobalAggregation4", rabbitAddr, outs["queue"])

	g.sessionHandler = sessionhandler.NewSessionHandler(aggregateQuery4, g.outputChannel)
}

func aggregate_4_func(accumulated_input string, new_input string) string {
	localAcc := make(map[string]map[string]uint64)

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
			userID := columns[0]
			storeID := columns[1]
			amountStr := columns[2]

			amount, err := strconv.ParseUint(amountStr, 10, 64)
			if err != nil {
				continue
			}

			if localAcc[storeID] == nil {
				localAcc[storeID] = make(map[string]uint64)
			}
			localAcc[storeID][userID] = amount
		}
	}

	if new_input != "" {
		lines := strings.Split(new_input, "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			columns := strings.Split(line, ",")
			if len(columns) != 3 {
				continue
			}
			userID := columns[0]
			storeID := columns[1]
			amountStr := columns[2]

			amount, err := strconv.ParseUint(amountStr, 10, 64)
			if err != nil {
				continue
			}

			if localAcc[storeID] == nil {
				localAcc[storeID] = make(map[string]uint64)
			}
			localAcc[storeID][userID] += amount
		}
	}

	var b strings.Builder
	for storeID, users := range localAcc {
		for userID, amount := range users {
			fmt.Fprintf(&b, "%s,%s,%d\n", userID, storeID, amount)
		}
	}

	return b.String()
}

func aggregateQuery4(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := single_packet_receiver.NewSinglePacketReceiver(sessionID, aggregate_4_func)

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

	totalAcc := make(map[string]map[string]uint64)

	lines := strings.Split(aggregated_packets, "\n")
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

		if totalAcc[storeID] == nil {
			totalAcc[storeID] = make(map[string]uint64)
		}
		totalAcc[storeID][userID] = amount
	}

	var b strings.Builder
	stores := make([]string, 0, len(totalAcc))
	for store := range totalAcc {
		stores = append(stores, store)
	}
	sort.Strings(stores)

	for _, store := range stores {
		users := totalAcc[store]

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
		size = min(len(sortedSlice), 3)

		for i := range size {
			fmt.Fprintf(&b, "%s,%s\n", store, sortedSlice[i].user)
		}
	}

	output := b.String()

	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{output})

	outputChannel <- newPkts[0]

	colas.WaitForAnswer(inputChannel)
	localReceiver.Clean()
}

func (g *aggregator4Global) Process() {
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
