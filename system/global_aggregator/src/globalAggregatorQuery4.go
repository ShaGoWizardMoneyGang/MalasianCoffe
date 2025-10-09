package global_aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"

	// "malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
)

type aggregator4Global struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[string]map[string]uint64

	receiver packet.PacketReceiver
}

func (g *aggregator4Global) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("PartialCountedUsers4", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation4", rabbitAddr)
	g.acc = make(map[string]map[string]uint64)

	g.receiver = packet.NewPacketReceiver("Aggregator 4")
}

// user_id | store_id | #transactions
func (g *aggregator4Global) Process(pkt packet.Packet) []packet.OutBoundMessage {
	g.receiver.ReceivePacket(pkt)

	if !g.receiver.ReceivedAll() {
		return nil
	}

	consolidatedInput := g.receiver.GetPayload()

	g.ingestBatch(consolidatedInput)

	final := g.flushAndBuild()
	if final == "" {
		return nil
	}

	g.receiver = packet.NewPacketReceiver("Aggregator 4")

	newPkts := packet.ChangePayload(pkt, []string{final})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: g.colaSalida,
		},
	}
}

// user_id | store_id | #transactions
func (g *aggregator4Global) ingestBatch(input string) {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas")
		}
		user_id, store_id, transaction_number := cols[0], cols[1], cols[2]

		amount, err := strconv.ParseUint(transaction_number, 10, 64)
		if err != nil {
			panic("tpv con formato inválido")
		}

		_, exists := g.acc[store_id]
		if !exists {
			g.acc[store_id] = make(map[string]uint64)
		}
		g.acc[store_id][user_id] += amount
	}
}

func (g *aggregator4Global) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}

	type kv struct {
		user   string
		amount uint64
	}

	var b strings.Builder
	for store, user2amount := range g.acc {
		var sortedSlice []kv
		for user, amount := range user2amount {
			sortedSlice = append(sortedSlice, kv{
				user,
				amount})
		}

		sort.Slice(sortedSlice, func(i, j int) bool {
			return sortedSlice[i].amount > sortedSlice[j].amount
		})

		var size int
		if len(sortedSlice) < 3 {
			size = len(sortedSlice)
		} else {
			size = 3
		}

		for i := 0; i < size; i++ {
			fmt.Fprintf(&b, "%s,%s\n", store, sortedSlice[i].user)
		}
	}

	g.acc = make(map[string]map[string]uint64)
	return b.String()
}

func (g *aggregator4Global) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}
