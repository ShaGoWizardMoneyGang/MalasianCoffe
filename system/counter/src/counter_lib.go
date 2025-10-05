package counter

import (
	"fmt"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/network"
	"strconv"
	"time"
)

func toYearMonth(dateStr string) string {
	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		return ""
	}
	return t.Format("2006-01")
}

// funcion comun entre los count de la query2
// Recibe item_id, subtotal, created_at o  item_id, quantity, created_at
// Devuelve year_month_created_at, item_id, sellings_qty o profit_sum
func countFunctionQuery2a(input string) string {
	rows := strings.Split(input, "\n")
	rows = rows[:len(rows)-1] // El split me genera 1 linea de mas vacia por el ultimo /n, la ignoro

	type key struct {
		yearMonth string
		itemID    string
	}
	counts := map[key]int64{}

	for _, r := range rows {
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
		key := key{yearMonth: toYearMonth(date), itemID: itemID}
		counts[key] += acumulatorValue
	}

	var b strings.Builder
	for k, val := range counts {
		fmt.Fprintf(&b, "%s,%s,%d\n", k.yearMonth, k.itemID, val)
	}
	return b.String()
}

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
		key := key{yearMonth: toYearMonth(date), itemID: itemID}
		counts[key] += acumulatorValue
	}

	var b strings.Builder
	for k, val := range counts {
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearMonth, k.itemID, val)
	}
	return b.String()
}

// ============================= CounterQuery2b ================================

type CounterQuery2b struct {
	colaEntradaFilteredTransactionItems *middleware.MessageMiddlewareQueue

	colaSalidaCountedSubtotal *middleware.MessageMiddlewareQueue
}

func (c *CounterQuery2b) Build(rabbitAddr string) {

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

func (c *CounterQuery2b) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactionItems
}

func (c *CounterQuery2b) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{countFunctionQuery2b(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaCountedSubtotal,
		},
	}

	return outBoundMessage
}

// ============================= CounterQuery2b ================================

// ============================= CounterQuery2a ================================

type CounterQuery2a struct {
	colaEntradaFilteredTransactionItems *middleware.MessageMiddlewareQueue

	colaSalidaCountedQuantity *middleware.MessageMiddlewareQueue
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
}

func (c *CounterQuery2a) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactionItems
}

func (c *CounterQuery2a) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{countFunctionQuery2a(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaCountedQuantity,
		},
	}

	return outBoundMessage
}

// ============================= CounterQuery2a ================================

// ============================= CounterQuery4 ================================

// NOTE: Gracias Mari por anadir estas lineas de input output
// Recibe transaction_id, store_id, user_id
// Devuelve user_id,store_id,cantidad por línea, ordenado por store_id y luego user_id para mantenerlo determinístico
func (c *CounterQuery4) countFunctionQuery4(input string) string {
	rows := strings.Split(input, "\n")
	rows = rows[:len(rows)-1] // El split me genera 1 linea de mas vacia por el ultimo /n, la ignoro

	// tengo un map donde voy contando las apariciones por store y user
	counts := map[query4.Key]int{}

	for _, r := range rows {

		cols := strings.Split(r, ",") // este comportamiento podria modularizarlo para todos
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		// NOTE: Estos no deberian estar vacios. Si lo estan, buscar error en el filter mapper
		storeID := cols[1]
		userID := cols[2]
		counts[query4.Key{Store: storeID, User: userID}]++
	}

	// ordeno, esto podria modularizarlo???
	keys := make([]query4.Key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	// NOTE: Quito el sort para reducir el costo. Para mantener determinisitco, anadir sort al resultado en los tests
	// sort.Slice(keys, func(i, j int) bool {
	// 	if keys[i].store == keys[j].store {
	// 		return keys[i].user < keys[j].user
	// 	}
	// 	return keys[i].store < keys[j].store
	// })

	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s,%s,%d\n", k.User, k.Store, counts[k])
	}
	return b.String()
}

type CounterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue

	colaSalidaPartialCountedUsers4 *middleware.MessageMiddlewareQueue
}

func (c *CounterQuery4) Build(rabbitAddr string) {

	colaEntrada, err := middleware.CreateQueue("FilteredTransactions4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactions4", err))
	}
	colaSalida, err := middleware.CreateQueue("PartialCountedUsers4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})

	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "PartialCountedUsers4", err))
	}

	c.colaEntradaFilteredTransactions4 = colaEntrada

	c.colaSalidaPartialCountedUsers4 = colaSalida
}

func (c *CounterQuery4) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactions4
}

func (c *CounterQuery4) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{c.countFunctionQuery4(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)
	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaPartialCountedUsers4,
		},
	}

	return outBoundMessage
}

// ============================= CounterQuery4 ================================

type Counter interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string)

	// Devuelve referencia de la cola de la cual tiene que consumir
	GetInput() *middleware.MessageMiddlewareQueue

	// Funcio que hace el filtrado
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

func CounterBuilder(counterName string, rabbitAddr string) Counter {
	var counter Counter
	switch strings.ToLower(counterName) {
	case "query4":
		counter = &CounterQuery4{}
	case "query2a":
		counter = &CounterQuery2a{}
	case "query2b":
		counter = &CounterQuery2b{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr)

	return counter
}
