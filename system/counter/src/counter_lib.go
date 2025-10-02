package counter

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/network"
	"strings"
)

// ============================= Counter2a init ================================
type counterQuery2a struct {
	colaEntradaFilteredTransactions2a *middleware.MessageMiddlewareQueue

	colaSalidaCountedTransactionItems2a *middleware.MessageMiddlewareQueue
}

func (c *counterQuery2a) countFunctionCounter2a(input string) string {
	panic("JAJA LLEGUÉ Y VOLAMOS EN PEDAZOS")
}

func (c *counterQuery2a) Build(rabbitAddr string) {
	c.colaEntradaFilteredTransactions2a = colas.InstanceQueue("FilteredTransactionItems2a", rabbitAddr)
	c.colaSalidaCountedTransactionItems2a = colas.InstanceQueue("CountedTransactionItems2a", rabbitAddr)
}

func (c *counterQuery2a) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactions2a
}
func (c *counterQuery2a) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{c.countFunctionCounter2a(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaCountedTransactionItems2a,
		},
	}

	return outBoundMessage
}

// ============================= Counter2a fin ================================

// ============================= CounterQuery4 ================================

// NOTE: Gracias Mari por anadir estas lineas de input output
// Recibe transaction_id, store_id, user_id
// Devuelve user_id,store_id,cantidad por línea, ordenado por store_id y luego user_id para mantenerlo determinístico
func (c *counterQuery4) countFunctionQuery4(input string) string {
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

type counterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue

	colaSalidaCountedUsers4 *middleware.MessageMiddlewareQueue
}

func (c *counterQuery4) Build(rabbitAddr string) {

	colaEntrada, err := middleware.CreateQueue("FilteredTransactions4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactions4", err))
	}

	colaSalida, err := middleware.CreateQueue("CountedUsers4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "CountedUsers4", err))
	}

	c.colaEntradaFilteredTransactions4 = colaEntrada

	c.colaSalidaCountedUsers4 = colaSalida
}

func (c *counterQuery4) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactions4
}

func (c *counterQuery4) Process(pkt packet.Packet) []packet.OutBoundMessage {
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
		counter = &counterQuery4{}
	case "counter2a":
		slog.Info("Building counter 2a")
		counter = &counterQuery2a{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr)

	return counter
}
