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

type key struct {
	yearHalf string // "YYYY-H1" o "YYYY-H2"
	storeID  string
}

type aggregator3Global struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[key]float64
}

func (g *aggregator3Global) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("SalidaQuery3", rabbitAddr) //IMPORTANTE ACA CAMBIAR POR DE LA QUE LEE EL JOIN
	//g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	g.acc = make(map[key]float64)
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

		k := key{yearHalf: yh, storeID: storeID}
		g.acc[k] += amount
	}
}

func (g *aggregator3Global) flushAndBuild() string {
	if len(g.acc) == 0 {
		return ""
	}

	keys := make([]key, 0, len(g.acc))
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

	g.acc = make(map[key]float64)
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
	switch strings.ToLower(name) {
	case "query3global":
		worker := &aggregator3Global{}
		worker.Build(rabbitAddr)
		return worker
	default:
		panic(fmt.Sprintf("Unknown global aggregator '%s'", name))
	}
}
