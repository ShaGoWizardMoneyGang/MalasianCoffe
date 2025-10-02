package global_aggregator

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"

	// "malasian_coffe/system/queries/query4"
	"malasian_coffe/utils/colas"
)

// ============================ AggregatorQuery3 ===============================
// ============================ AggregatorQuery3 ===============================

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

		quantity, err := strconv.ParseFloat(quantityStr, 64)
		if err != nil {
			panic("quantity con formato inválido")
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
		quantity float64
	})

	for k, value := range g.acc {
		yearMonth := k.yearMonth
		if current, exists := monthlyMax[yearMonth]; !exists || value > current.quantity {
			monthlyMax[yearMonth] = struct {
				itemID   string
				quantity float64
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
		fmt.Fprintf(&b, "%s,%s,%.2f\n", month, maxItem.itemID, maxItem.quantity)
	}

	g.acc = make(map[keyQuery2a]float64)

	result := b.String()
	return result
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
		fmt.Println("No recibidi ningun dato. Raro. Ojo")
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

	// // NOTE: Usar despues de la entrega
	var b strings.Builder
	for k, val := range g.acc {
		yearHalf := k.yearHalf
		storeID := k.storeID
		value := val

		fmt.Fprintf(&b, "%s,%s,%.2f\n", yearHalf, storeID, value)
	}

	g.acc = make(map[keyQuery3]float64)
	return b.String()
}

func (g *aggregator3Global) Process(pkt packet.Packet) []packet.OutBoundMessage {
	slog.Info("Processing packet in Global Aggregator 3")
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
	var globalAggregator GlobalAggregator
	switch strings.ToLower(name) {
	case "query2aglobal":
		globalAggregator = &aggregator2aGlobal{}
	case "query2bglobal":
		globalAggregator = &aggregator2bGlobal{}
	case "query3global":
		globalAggregator = &aggregator3Global{}
	case "query4global":
		globalAggregator = &aggregator4Global{}
	default:
		panic(fmt.Sprintf("Unknown global aggregator '%s'", name))
	}
	globalAggregator.Build(rabbitAddr)
	return globalAggregator
}
