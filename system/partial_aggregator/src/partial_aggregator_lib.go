package partial_aggregator

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Recibe: "store_id,final_amount,created_at" por línea
// Devuelve: "YYYY-MM,store_id,tpv" ordenado por YYYY-MM y luego store_id
func aggregator3ByMonthTPV(input string) string {
	const layout = "2006-01-02 15:04:05"

	type key struct {
		yearAndMonth string
		storeID      string
	}
	acc := make(map[key]float64)

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas: store_id,final_amount,created_at")
		}

		storeID := cols[0]

		amount, err := strconv.ParseFloat(cols[1], 64)
		if err != nil {
			panic("final_amount con formato inválido")
		}

		ts, err := time.Parse(layout, cols[2])
		if err != nil {
			panic("created_at con formato inválido")
		}

		ym := ts.Format("2006-01")
		k := key{yearAndMonth: ym, storeID: storeID}
		acc[k] += amount
	}

	// ordeno por mes y después por store_id
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

type PartialAggregator interface {
	Build(rabbitAddr string)
	GetInput() *middleware.MessageMiddlewareQueue
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

type aggregator3Partial struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
}

func (a *aggregator3Partial) Build(rabbitAddr string) {
	// mismas colas que las de antes
	a.colaEntrada = colas.InstanceQueue("FilteredTransactions3", rabbitAddr)
	a.colaSalida = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
}

func (a *aggregator3Partial) GetInput() *middleware.MessageMiddlewareQueue {
	return a.colaEntrada
}

func (a *aggregator3Partial) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()
	slog.Debug("Aggregator3Partial.Process: recibí payload")

	result := aggregator3ByMonthTPV(input)

	newPkts := packet.ChangePayload(pkt, []string{result})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: a.colaSalida,
		},
	}
}

func PartialAggregatorBuilder(name string, rabbitAddr string) PartialAggregator {
	switch strings.ToLower(name) {
	case "query3":
		partialAgg := &aggregator3Partial{}
		partialAgg.Build(rabbitAddr)
		return partialAgg
	default:
		panic(fmt.Sprintf("Funcion desconocida '%s'", name))
	}
}
