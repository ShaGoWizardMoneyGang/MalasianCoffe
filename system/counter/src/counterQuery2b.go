package counter

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
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
	colaEntradaFilteredTransactionItems *middleware.MessageMiddlewareQueue

	colaSalidaCountedSubtotal *middleware.MessageMiddlewareQueue
}

func (c *counterQuery2b) Build(rabbitAddr string, queueAmounts map[string] uint64) {

	colaEntrada, err := middleware.CreateQueue("FilteredTransactionItems2b", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactionItems2b", err))
	}
	//CountedItems2a
	colaSalida, err := middleware.CreateQueue("CountedItems2b", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "CountedItems2b", err))
	}

	c.colaEntradaFilteredTransactionItems = colaEntrada

	c.colaSalidaCountedSubtotal = colaSalida
}

func (c *counterQuery2b) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactionItems
}

func (c *counterQuery2b) Process(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{countFunctionQuery2b(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)

	outBoundMessage := []colas.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaCountedSubtotal,
		},
	}

	return outBoundMessage
}
