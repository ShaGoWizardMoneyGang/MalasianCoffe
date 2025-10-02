package counter

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"strconv"
	"strings"
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
func countFunctionQuery2(input string) string {
	rows := strings.Split(input, "\n")
	rows = rows[:len(rows)-1] // El split me genera 1 linea de mas vacia por el ultimo /n, la ignoro

	type key struct {
		yearMonth string
		itemID    string
	}
	// tengo un map donde voy contando las apariciones por mes e item
	counts := map[key]int{}

	for _, r := range rows {

		cols := strings.Split(r, ",") // este comportamiento podria modularizarlo para todos
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		// NOTE: Estos no deberian estar vacios. Si lo estan, buscar error en el filter mapper
		itemID := cols[0]
		acumulatorValue, _ := strconv.Atoi(cols[1])
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

	counted_result := []string{countFunctionQuery2(input)}

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

	counted_result := []string{countFunctionQuery2(input)}

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

	type key struct {
		store string
		user  string
	}
	// tengo un map donde voy contando las apariciones por store y user
	counts := map[key]int{}

	for _, r := range rows {

		cols := strings.Split(r, ",") // este comportamiento podria modularizarlo para todos
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		// NOTE: Estos no deberian estar vacios. Si lo estan, buscar error en el filter mapper
		storeID := cols[1]
		userID := cols[2]
		counts[key{store: storeID, user: userID}]++
	}

	// ordeno, esto podria modularizarlo???
	keys := make([]key, 0, len(counts))
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
		fmt.Fprintf(&b, "%s,%s,%d\n", k.user, k.store, counts[k])
	}
	return b.String()
}

type CounterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue

	colaSalidaCountedUsers4 *middleware.MessageMiddlewareQueue
}

func (c *CounterQuery4) Build(rabbitAddr string) {

	colaEntrada, err := middleware.CreateQueue("FilteredTransactions4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactions4", err))
	}
	//CountedUsers4
	colaSalida, err := middleware.CreateQueue("CountedUsers4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "CountedUsers4", err))
	}

	c.colaEntradaFilteredTransactions4 = colaEntrada

	c.colaSalidaCountedUsers4 = colaSalida
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
			ColaSalida: c.colaSalidaCountedUsers4,
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
	switch counterName {
	case "counter4":
		counter = &CounterQuery4{}
	case "counter2a":
		counter = &CounterQuery2a{}
	case "counter2b":
		counter = &CounterQuery2b{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr)

	return counter
}
