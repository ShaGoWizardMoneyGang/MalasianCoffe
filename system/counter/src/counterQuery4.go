package counter

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/system/queries/query4"
	watchdog "malasian_coffe/system/watchdog/src"
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
	packet_channel chan colas.PacketMessage

	colaEntradaFilteredTransactions4   *middleware.MessageMiddlewareQueue
	exchangeSalidaPartialCountedUsers4 *middleware.MessageMiddlewareExchange
}

func (c *counterQuery4) Build(rabbitAddr string, queueAmounts map[string]uint64) {
	c.packet_channel = make(chan colas.PacketMessage)
	colaEntrada, err := middleware.CreateQueue("FilteredTransactions4", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", "FilteredTransactions4", err))
	}

	c.colaEntradaFilteredTransactions4 = colaEntrada

	c.exchangeSalidaPartialCountedUsers4 = colas.InstanceExchange("PartialCountedUsers4", rabbitAddr, queueAmounts["queue"])
}

func (c *counterQuery4) Process() {
	slog.Info("Arranca procesamiento de counterQuery4")

	go colas.InputQueue(c.colaEntradaFilteredTransactions4, c.packet_channel)

	watchdogListener := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdogListener.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-c.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()
			counted_result := []string{c.countFunctionQuery4(input)}
			newPayload := packet.ChangePayload(pkt, counted_result)
			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: c.exchangeSalidaPartialCountedUsers4,
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
