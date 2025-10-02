package counter

import (
	"fmt"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"malasian_coffe/system/queries/query4"
)

// ============================= CounterQuery4 ================================

// NOTE: Gracias Mari por anadir estas lineas de input output
// Recibe transaction_id, store_id, user_id
// Devuelve user_id,store_id,cantidad por línea, ordenado por store_id y luego user_id para mantenerlo determinístico
func (c *counterQuery4) countFunctionQuery4(input string) string {
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

type counterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue

	colaSalidaPartialCountedUsers4 *middleware.MessageMiddlewareQueue
}

func (c *counterQuery4) Build(rabbitAddr string) {

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

func (c *counterQuery4) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactions4
}

func (c *counterQuery4) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{c.countFunctionQuery4(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet: newPayload[0],
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
	var counter Counter;
	switch counterName {
	case "counter4":
		counter = &counterQuery4{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr)

	return counter
}
