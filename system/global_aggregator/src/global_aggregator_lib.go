package global_aggregator

import (
	"fmt"
	"malasian_coffe/packets/packet"
	"sort"
	"strconv"
	"strings"
)

type Aggregator struct {
}

// ESTO YA RECIBE LAS COSITAS PARCIALES AAAAAAAAAAAAA
func (a *Aggregator) AggregatorGlobal3ByMonthTPV(input string) string {
	type key struct {
		yearAndMonth string
		storeID      string
	}

	acc := make(map[key]float64)

	lines := strings.Split(strings.TrimSpace(input), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas: YYYY-MM,store_id,tpv")
		}

		ym := cols[0]
		storeID := cols[1]
		amountStr := cols[2]
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			panic("tpv con formato inválido")
		}

		k := key{yearAndMonth: ym, storeID: storeID}
		acc[k] += amount
	}

	// ordenar por mes y luego por store_id
	keys := make([]key, 0, len(acc))
	for k := range acc {
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
		fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearAndMonth, k.storeID, acc[k])
	}
	return b.String()
}

func (a *Aggregator) Process(pkt packet.Packet, function string) []packet.Packet {
	input := pkt.GetPayload()
	var salida string
	switch strings.ToLower(function) {
	case "agregator3globalbymonthtpv":
		salida = a.AggregatorGlobal3ByMonthTPV(input)
	default:
		panic("Funcion desconocida")
	}
	return packet.ChangePayload(pkt, []string{salida})
}
