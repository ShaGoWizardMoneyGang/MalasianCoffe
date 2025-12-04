package counter

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/network"
	"malasian_coffe/utils/parser"
	"strconv"
	"strings"
)

// funcion comun entre los count de la query2
// Recibe item_id, subtotal, created_at o  item_id, quantity, created_at
// Devuelve year_month_created_at, item_id, sellings_qty o profit_sum
func countFunctionQuery2b(input string) string {
	rows := strings.Split(input, "\n")
	rows = rows[:len(rows)-1] // El split me genera 1 linea de mas vacia por el ultimo /n, la ignoro

	type key struct {
		yearMonth string
		itemID    string
	}
	counts := map[key]float64{}

	for _, r := range rows {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		itemID := cols[0]
		acumulatorValue, err := strconv.ParseFloat(cols[1], 64)
		if err != nil {
			panic(fmt.Sprintf("Error parsing accumulator value '%s': %s", cols[1], err))
		}
		date := cols[2]
		key := key{yearMonth: parser.ToYearMonth(date), itemID: itemID}
		counts[key] += acumulatorValue
	}

	var b strings.Builder
	for k, val := range counts {
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearMonth, k.itemID, val)
	}
	return b.String()
}

// ============================= CounterQuery2b ================================

type counterQuery2b struct {
	packet_channel chan colas.PacketMessage

	colaEntradaFilteredTransactionItems *middleware.MessageMiddlewareQueue

	exchangeSalidaCountedSubtotal *middleware.MessageMiddlewareExchange
}

func (c *counterQuery2b) Build(rabbitAddr string, queueAmounts map[string]uint64) {
	c.packet_channel = make(chan colas.PacketMessage)
	colaEntrada, err := middleware.CreateQueue("FilteredTransactionItems2b", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactionItems2b", err))
	}
	//CountedItems2a
	c.colaEntradaFilteredTransactionItems = colaEntrada

	c.exchangeSalidaCountedSubtotal = colas.InstanceExchange("CountedItems2b", rabbitAddr, queueAmounts["queue"])
}

func (c *counterQuery2b) Process() {
	slog.Info("Arranca procesamiento de counterQuery2b")

	go colas.InputQueue(c.colaEntradaFilteredTransactionItems, c.packet_channel)

	watchdogListener := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdogListener.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-c.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()
			counted_result := []string{countFunctionQuery2b(input)}
			newPayload := packet.ChangePayload(pkt, counted_result)
			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: c.exchangeSalidaCountedSubtotal,
				},
			}
			for _, outbound := range outBoundMessages {
				cola := outbound.ColaSalida
				packet := outbound.Packet
				cola.Send(packet)
			}
			message.Ack(false)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			watchdogListener.Pong(IP)
		}
	}
}
