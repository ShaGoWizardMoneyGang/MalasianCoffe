package counter

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"malasian_coffe/utils/parser"
	"strconv"
	"strings"
)

// Recibe item_id, subtotal, created_at o  item_id, quantity, created_at
// Devuelve year_month_created_at, item_id, sellings_qty o profit_sum
func countFunctionQuery2a(input string) string {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]

	type key struct {
		yearMonth string
		itemID    string
	}
	counts := map[key]int64{}

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		itemID := cols[0]
		acumulatorValue, err := strconv.ParseInt(cols[1], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Error parsing accumulator value '%s': %s", cols[1], err))
		}
		date := cols[2]
		key := key{yearMonth: parser.ToYearMonth(date), itemID: itemID}
		counts[key] += acumulatorValue
	}

	var b strings.Builder
	for k, val := range counts {
		fmt.Fprintf(&b, "%s,%s,%d\n", k.yearMonth, k.itemID, val)
	}
	return b.String()
}

// ============================= CounterQuery2a ================================

type CounterQuery2a struct {
	colaEntradaFilteredTransactionItems *middleware.MessageMiddlewareQueue
	colaSalidaCountedQuantity           *middleware.MessageMiddlewareQueue
	receiver                            packet.PacketReceiver
}

func (c *CounterQuery2a) Build(rabbitAddr string) {
	colaEntrada, err := middleware.CreateQueue("FilteredTransactionItems2a", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactionItems2a", err))
	}
	//CountedItems2a
	colaSalida, err := middleware.CreateQueue("CountedItems2a", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "CountedItems2a", err))
	}

	c.colaEntradaFilteredTransactionItems = colaEntrada
	c.colaSalidaCountedQuantity = colaSalida

	c.receiver = packet.NewPacketReceiver()
}

func (c *CounterQuery2a) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactionItems
}

func (c *CounterQuery2a) Process(pkt packet.Packet) []packet.OutBoundMessage {
	c.receiver.ReceivePacket(pkt)

	if !c.receiver.ReceivedAll() {
		fmt.Println("Aún no se han recibido todos los paquetes")
		return nil
	}

	consolidatedInput := c.receiver.GetPayload()

	counted_result := []string{countFunctionQuery2a(consolidatedInput)}

	c.receiver = packet.NewPacketReceiver()

	newPayload := packet.ChangePayload(pkt, counted_result)

	return []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaCountedQuantity,
		},
	}
}

// ============================= CounterQuery2a ================================
