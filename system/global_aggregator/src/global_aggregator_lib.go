package global_aggregator

import (
	"fmt"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	// "malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
)

// ============================ AggregatorQuery3 ===============================
// ============================ AggregatorQuery3 ===============================

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
		fmt.Println("No recibidi ningun dato. Raro. Ojo")
		return ""
	}


	var b strings.Builder
	for k, val := range g.acc {
		yearHalf := k.yearHalf
		storeID  := k.storeID
		value    := val

		fmt.Fprintf(&b, "%s,%s,%.2f\n", yearHalf, storeID, value)
	}

	// keys := make([]keyQuery3, 0, len(g.acc))
	// for k := range g.acc {
	// 	keys = append(keys, k)
	// }
	// QUESTION(fabri): Por que sorteamos?
	// sort.Slice(keys, func(i, j int) bool {
	// 	if keys[i].yearHalf == keys[j].yearHalf {
	// 		return keys[i].storeID < keys[j].storeID
	// 	}
	// 	return keys[i].yearHalf < keys[j].yearHalf
	// })

	// Resetear
	g.acc = make(map[keyQuery3]float64)
	return b.String()
}

func (g *aggregator3Global) Process(pkt packet.Packet) []packet.OutBoundMessage {
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
// ============================ AggregatorQuery3 ===============================

type GlobalAggregator interface {
	Build(rabbitAddr string)
	GetInput() *middleware.MessageMiddlewareQueue
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

func GlobalAggregatorBuilder(name, rabbitAddr string) GlobalAggregator {
	var worker GlobalAggregator
	switch strings.ToLower(name) {
	case "query3global":
		worker = &aggregator3Global{}
	case "query4global":
		worker = &aggregator4Global{}
	default:
		panic(fmt.Sprintf("Unknown global aggregator '%s'", name))
	}
	worker.Build(rabbitAddr)
	return worker
}
