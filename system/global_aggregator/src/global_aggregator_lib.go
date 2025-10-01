package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"sort"
	"strconv"
	"strings"
)

type key struct {
	yearAndMonth string
	storeID      string
}

type GlobalAggregator struct {
	acc map[key]float64
}

func NewAggregator() *GlobalAggregator {
	return &GlobalAggregator{acc: make(map[key]float64)}
}

// batch del parcial "YYYY-MM,store_id,tpv"
func (a *GlobalAggregator) ingestBatch(input string) {
	lines := strings.SplitSeq(strings.TrimSpace(input), "\n")
	for line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas: YYYY-MM,store_id,tpv")
		}
		ym := strings.TrimSpace(cols[0])
		storeID := strings.TrimSpace(cols[1])
		amountStr := strings.TrimSpace(cols[2])

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			panic("tpv con formato inválido")
		}

		k := key{yearAndMonth: ym, storeID: storeID}
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
		if keys[i].yearAndMonth == keys[j].yearAndMonth {
			return keys[i].storeID < keys[j].storeID
		}
		return keys[i].yearAndMonth < keys[j].yearAndMonth
	})

	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearAndMonth, k.storeID, a.acc[k])
	}

	a.acc = make(map[key]float64)
	return b.String()
}

func (a *GlobalAggregator) Process(pkt packet.Packet, function string) []packet.Packet {
	input := strings.TrimSpace(pkt.GetPayload())
	switch strings.ToLower(function) {
	case "agregator3globalbymonthtpv":
		// 	EOF
		if pkt.IsEOF() {
			final := a.flushAndBuild()
			if final == "" {
				return nil
			}
			fmt.Print(final)
			return packet.ChangePayload(pkt, []string{final})
		}

		// caso que no es eof
		a.ingestBatch(input)
		return nil

	default:
		panic("Funcion desconocida")
	}
}
