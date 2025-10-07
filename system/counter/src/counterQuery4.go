package counter

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/network"
	"strings"
)

// NOTE: Gracias Mari por anadir estas lineas de input output
// Recibe transaction_id, store_id, user_id
// Devuelve user_id,store_id,cantidad por línea, ordenado por store_id y luego user_id para mantenerlo determinístico
func (c *CounterQuery4) countFunctionQuery4(input string) string {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1] // Remover última línea vacía

	// tengo un map donde voy contando las apariciones por store y user
	counts := map[query4.Key]int{}

	for _, line := range lines {

		cols := strings.Split(line, ",") // este comportamiento podria modularizarlo para todos
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

type CounterQuery4 struct {
	colaEntradaFilteredTransactions4 *middleware.MessageMiddlewareQueue
	colaSalidaPartialCountedUsers4   *middleware.MessageMiddlewareQueue

	receiver packet.PacketReceiver
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

	c.receiver = packet.NewPacketReceiver()
}

func (c *CounterQuery4) GetInput() *middleware.MessageMiddlewareQueue {
	return c.colaEntradaFilteredTransactions4
}

func (c *CounterQuery4) Process(pkt packet.Packet) []packet.OutBoundMessage {
	c.receiver.ReceivePacket(pkt)

	if !c.receiver.ReceivedAll() {
		fmt.Println("Aún no se han recibido todos los paquetes")
		return nil
	}

	consolidatedInput := c.receiver.GetPayload()

	counted_result := []string{c.countFunctionQuery4(consolidatedInput)}

	c.receiver = packet.NewPacketReceiver()

	newPayload := packet.ChangePayload(pkt, counted_result)

	return []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: c.colaSalidaPartialCountedUsers4,
		},
	}
}
