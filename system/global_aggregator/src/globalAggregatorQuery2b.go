package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"sort"
	"strconv"
	"strings"
)

// AggregatorGLobal de la query2a ES LA SUBTOTAL
type keyQuery2b struct {
	yearMonth string
	itemID    string
}

type aggregator2bGlobal struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery2b]float64
}

func (g *aggregator2bGlobal) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("CountedItems2b", rabbitAddr)
	// aca va GlobalAggregation2b
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2b", rabbitAddr)
	g.acc = make(map[keyQuery2b]float64)
}

func (g *aggregator2bGlobal) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}

func (g *aggregator2bGlobal) ingestBatch(input string) {
	lines := strings.SplitSeq(input, "\n")
	for line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas")
		}
		yearMonth := cols[0]
		itemID := cols[1]
		subtotalStr := cols[2]

		subtotal, err := strconv.ParseFloat(subtotalStr, 64)
		if err != nil {
			panic("subtotal con formato inválido")
		}

		k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
		g.acc[k] += subtotal
	}
}

func (g *aggregator2bGlobal) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}

	monthlyMax := make(map[string]struct {
		itemID   string
		subtotal float64
	})

	for k, value := range g.acc {
		yearMonth := k.yearMonth

		if current, exists := monthlyMax[yearMonth]; !exists || value > current.subtotal {
			monthlyMax[yearMonth] = struct {
				itemID   string
				subtotal float64
			}{
				itemID:   k.itemID,
				subtotal: value,
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
		fmt.Fprintf(&b, "%s,%s,%.2f\n", month, maxItem.itemID, maxItem.subtotal)
	}

	g.acc = make(map[keyQuery2b]float64)
	return b.String()
}

func (g *aggregator2bGlobal) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	isEOF := pkt.IsEOF()
	g.ingestBatch(input)
	if !isEOF {
		return nil
	}

	final := g.flushAndBuild()
	if final == "" {
		return nil
	}

	newPkts := packet.ChangePayload(pkt, []string{final})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: g.colaSalida,
		},
	}
}
