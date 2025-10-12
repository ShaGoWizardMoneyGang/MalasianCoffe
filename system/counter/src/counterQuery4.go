package counter

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/network"
	"strings"
)

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

	var b strings.Builder
	for k, count := range counts {
		fmt.Fprintf(&b, "%s,%s,%d\n", k.User, k.Store, count)
	}
	return b.String()
}

type counterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue

	colaSalidaPartialCountedUsers4 *middleware.MessageMiddlewareQueue
}

func (c *counterQuery4) Build(rabbitAddr string, queueAmounts map[string] uint64) {

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

func (c *counterQuery4) Process(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()

	counted_result := []string{c.countFunctionQuery4(input)}

	newPayload := packet.ChangePayload(pkt, counted_result)
	outBoundMessage := []colas.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaPartialCountedUsers4,
		},
	}

	return outBoundMessage
}
