package global_aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_receiver"
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

func updateAccumulator(consolidatedInput string, localAcc map[string]map[string]uint64) {
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
}

func buildOutput(localAcc map[string]map[string]uint64) string {
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
		size = min(len(sortedSlice), 3)

		for i := range size {
			fmt.Fprintf(&b, "%s,%s\n", store, sortedSlice[i].user)
		}
	}
	return b.String()
}

func aggregateQuery4(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := packet_receiver.NewPacketReceiver("agregador-global-4")
	localAcc := make(map[string]map[string]uint64)

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

	updateAccumulator(consolidatedInput, localAcc)

	output := buildOutput(localAcc)

	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{output})
	outputChannel <- newPkts[0]

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
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("GlobalAggregator 4 received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
