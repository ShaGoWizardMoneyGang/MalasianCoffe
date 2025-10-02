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

type GlobalAggregator interface {
	Build(rabbitAddr string)
	GetInput() *middleware.MessageMiddlewareQueue
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

// AggregatorGLobal de la query2a ES LA QUANTITY
type keyQuery2a struct {
	yearMonth string
	itemID    string
}

type aggregator2aGlobal struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery2a]float64
}

func (g *aggregator2aGlobal) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("CountedItems2a", rabbitAddr)
	// aca va GlobalAggregation2a
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)
	g.acc = make(map[keyQuery2a]float64)
}

func (g *aggregator2aGlobal) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}

func (g *aggregator2aGlobal) ingestBatch(input string) {
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
		quantityStr := cols[2]

		amount, err := strconv.ParseFloat(quantityStr, 64)
		if err != nil {
			panic("quantity con formato inválido")
		}

		k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
		g.acc[k] += amount
	}
}

func (g *aggregator2aGlobal) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}

	var b strings.Builder
	for k, value := range g.acc {
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearMonth, k.itemID, value)
	}
	g.acc = make(map[keyQuery2a]float64)
	return b.String()
}

func (g *aggregator2aGlobal) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	isEOF := pkt.IsEOF()

	if !isEOF {
		g.ingestBatch(input)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type keyQuery3 struct {
	yearHalf string // "YYYY-H1" o "YYYY-H2"
	storeID  string
}

type aggregator3Global struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery3]float64
}

func (g *aggregator3Global) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	//g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	g.acc = make(map[keyQuery3]float64)
}

func (g *aggregator3Global) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}

func (g *aggregator3Global) ingestBatch(input string) {
	lines := strings.SplitSeq(input, "\n")
	for line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas")
		}
		yh := cols[0]
		storeID := cols[1]
		amountStr := cols[2]

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			panic("tpv con formato inválido")
		}

		k := keyQuery3{yearHalf: yh, storeID: storeID}
		g.acc[k] += amount
	}
}

func (g *aggregator3Global) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}

	keys := make([]keyQuery3, 0, len(g.acc))
	for k := range g.acc {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].yearHalf == keys[j].yearHalf {
			return keys[i].storeID < keys[j].storeID
		}
		return keys[i].yearHalf < keys[j].yearHalf
	})

	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearHalf, k.storeID, g.acc[k])
	}

	g.acc = make(map[keyQuery3]float64)
	return b.String()
}

func (g *aggregator3Global) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	isEOF := pkt.IsEOF() || strings.EqualFold(input, "EOF")

	if !isEOF {
		g.ingestBatch(input)
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

func GlobalAggregatorBuilder(name, rabbitAddr string) GlobalAggregator {
	var globalAggregator GlobalAggregator
	switch strings.ToLower(name) {
	case "query3global":
		globalAggregator = &aggregator3Global{}
	case "query2aglobal":
		globalAggregator = &aggregator2aGlobal{}
	default:
		panic(fmt.Sprintf("Unknown global aggregator '%s'", name))
	}
	globalAggregator.Build(rabbitAddr)
	return globalAggregator
}
