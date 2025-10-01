package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"sort"
	"strconv"
	"strings"
)

type key struct {
	yearHalf string
	storeID  string
}

type GlobalAggregator struct {
	acc map[key]float64
}

func NewAggregator() *GlobalAggregator {
	return &GlobalAggregator{acc: make(map[key]float64)}
}

// batch del parcial "YYYY-H{1|2},store_id,tpv"
func (a *GlobalAggregator) ingestBatch(input string) {
	lines := strings.Split(strings.TrimSpace(input), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas: YYYY-H{1|2},store_id,tpv")
		}
		yh := strings.TrimSpace(cols[0])      // YYYY-H1 o YYYY-H2
		storeID := strings.TrimSpace(cols[1]) // store_id
		amountStr := strings.TrimSpace(cols[2])

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			panic("tpv con formato inválido")
		}

		k := key{yearHalf: yh, storeID: storeID}
		a.acc[k] += amount
	}
}

func (a *GlobalAggregator) flushAndBuild() string {
	if len(a.acc) == 0 {
		return ""
	}

	keys := make([]key, 0, len(a.acc))
	for k := range a.acc {
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
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearHalf, k.storeID, a.acc[k])
	}

	a.acc = make(map[key]float64)
	return b.String()
}

func (a *GlobalAggregator) Process(pkt packet.Packet, function string) []packet.Packet {
	input := strings.TrimSpace(pkt.GetPayload())
	switch strings.ToLower(function) {
	case "agregator3globalbysemestertpv":
		if pkt.IsEOF() {
			final := a.flushAndBuild()
			if final == "" {
				return nil
			}
			return packet.ChangePayload(pkt, []string{final})
		}
		a.ingestBatch(input)
		return nil

	default:
		panic("Funcion desconocida")
	}
}
