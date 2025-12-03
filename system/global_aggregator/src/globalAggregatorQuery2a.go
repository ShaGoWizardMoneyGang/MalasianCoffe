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

func aggregate_2a_func(accumulated_input string, new_input string) string {
	localAcc := make(map[keyQuery2a]int64)

	// Si no está vacío el accumulated_input, convierto a diccionario el contenido
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
			quantityStr := columns[2]

			quantity, err := strconv.ParseInt(quantityStr, 10, 64)
			if err != nil {
				panic("Quantity con formato inválido")
			}

			k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
			localAcc[k] = quantity
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
		quantityStr := columns[2]

		quantity, err := strconv.ParseInt(quantityStr, 10, 64)
		if err != nil {
			panic("Quantity con formato inválido")
		}

		k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
		localAcc[k] += quantity

	}

	var b strings.Builder
	for key, quantity := range localAcc {
		fmt.Fprintf(&b, "%s,%s,%d\n", key.yearMonth, key.itemID, quantity)
	}

	return b.String()

}

func aggregateQuery2a(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := single_packet_receiver.NewSinglePacketReceiver(sessionID, aggregate_2a_func)

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

	totalAcc := make(map[keyQuery2a]int64)

	// Si no está vacío el accumulated_input, convierto a diccionario el contenido
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
		quantityStr := columns[2]

		quantity, err := strconv.ParseInt(quantityStr, 10, 64)
		if err != nil {
			panic("Quantity con formato inválido")
		}

		k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
		totalAcc[k] = quantity
	}

	monthlyMax := make(map[string]struct {
		itemID   string
		quantity int64
	})

	for k, value := range totalAcc {
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

	var topQuantityProducts strings.Builder
	months := make([]string, 0, len(monthlyMax))
	for month := range monthlyMax {
		months = append(months, month)
	}
	sort.Strings(months)

	for _, month := range months {
		maxItem := monthlyMax[month]
		fmt.Fprintf(&topQuantityProducts, "%s,%s,%d\n", month, maxItem.itemID, maxItem.quantity)
	}

	newPkts := packet.ChangePayloadGlobalAggregator(last_packet, "transaction_items", []string{topQuantityProducts.String()})

	outputChannel <- newPkts[0]

	colas.WaitForAnswer(inputChannel)
	localReceiver.Clean()
}

func (g *aggregator2aGlobal) Process() {
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
			fmt.Println("GlobalAggregator 2a received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
