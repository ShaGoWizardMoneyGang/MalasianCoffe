package aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Aggregator struct {
}

// Recibe store_id,final_amount,created_at y devuelve YYYY-MM,store_id,tpv ordenada
func (a *Aggregator) Aggregator3ByMonthTPV(input string) string {
	const layout = "2006-01-02 15:04:05"

	type key struct {
		yearAndMonth string
		storeID      string
	}
	accumulator := make(map[key]float64)

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas")
		}

		storeID := cols[0]
		amountStr := cols[1]
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			panic("final_amount con formato invalido")
		}

		tsStr := cols[2]
		ts, err := time.Parse(layout, tsStr)
		if err != nil {
			panic("created_at con formato invalido")
		}

		ym := ts.Format("2006-01")
		k := key{yearAndMonth: ym, storeID: storeID}
		accumulator[k] += amount
	}

	// ordeno por mes y despues por ttpv
	keys := make([]key, 0, len(accumulator))
	for k := range accumulator {
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
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearAndMonth, k.storeID, accumulator[k])
	}
	return b.String()
}

func (a *Aggregator) Process(pkt packet.Packet, function string) []packet.Packet {
	input := pkt.GetPayload()
	var salida string
	switch strings.ToLower(function) {
	case "AggregatorByMonthTPV":
		salida = a.Aggregator3ByMonthTPV(input)
	default:
		panic("Funcion desconocida")
	}
	return packet.ChangePayload(pkt, []string{salida})
}
