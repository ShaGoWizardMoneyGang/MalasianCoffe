package global_aggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
)

// AggregatorGLobal de la query2a ES LA QUANTITY
type keyQuery2a struct {
	yearMonth string
	itemID    string
}

type aggregator2aGlobal struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery2a]int64

	receiver packet.PacketReceiver
}

func (g *aggregator2aGlobal) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("CountedItems2a", rabbitAddr)
	// aca va GlobalAggregation2a
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)
	g.acc = make(map[keyQuery2a]int64)

	g.receiver = packet.NewPacketReceiver("Agregador global")
}

func (g *aggregator2aGlobal) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}

func (g *aggregator2aGlobal) ingestBatch(input string) {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			bitacora.Debug("Se esperaban 3 columnas")
			continue
		}
		yearMonth := cols[0]
		itemID := cols[1]
		quantityStr := cols[2]

		quantity, err := strconv.ParseInt(quantityStr, 10, 64)
		if err != nil {
			panic("Quantity con formato inválido")
		}

		k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
		g.acc[k] += quantity
	}
}

func (g *aggregator2aGlobal) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}
	monthlyMax := make(map[string]struct {
		itemID   string
		quantity int64
	})

	for k, value := range g.acc {
		yearMonth := k.yearMonth
		if current, exists := monthlyMax[yearMonth]; !exists || value > current.quantity {
			monthlyMax[yearMonth] = struct {
				itemID   string
				quantity int64
			}{
				itemID:   k.itemID,
				quantity: value,
			}
		}
	}
	var b strings.Builder

	months := make([]string, 0, len(monthlyMax))
	for month := range monthlyMax {
		months = append(months, month)
	}
	sort.Strings(months)

	for _, month := range months {
		maxItem := monthlyMax[month]
		fmt.Fprintf(&b, "%s,%s,%d\n", month, maxItem.itemID, maxItem.quantity)
	}

	g.acc = make(map[keyQuery2a]int64)

	result := b.String()
	return result
}

func (g *aggregator2aGlobal) Process(pkt packet.Packet) []packet.OutBoundMessage {

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

	g.receiver = packet.NewPacketReceiver("Agregator 2a")

	newPkts := packet.ChangePayload(pkt, []string{final})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: g.colaSalida,
		},
	}
}
