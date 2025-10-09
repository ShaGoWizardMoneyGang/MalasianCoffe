package global_aggregator

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"sort"
	"strconv"
	"strings"
)

type keyQuery3 struct {
	yearHalf string // "YYYY-H1" o "YYYY-H2"
	storeID  string
}

type aggregator3Global struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery3]float64

	receiver packet.PacketReceiver
}

func (g *aggregator3Global) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	//g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	g.acc = make(map[keyQuery3]float64)

	g.receiver = packet.NewPacketReceiver("Aggregator 3")
}

func (g *aggregator3Global) GetInput() *middleware.MessageMiddlewareQueue {
	return g.colaEntrada
}

func (g *aggregator3Global) ingestBatch(input string) {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
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

	g.receiver.ReceivePacket(pkt)

	if !g.receiver.ReceivedAll() {
		slog.Info("Aún no se han recibido todos los paquetes")
		return nil
	}

	consolidatedInput := g.receiver.GetPayload()

	g.ingestBatch(consolidatedInput)

	final := g.flushAndBuild()
	if final == "" {
		return nil
	}

	g.receiver = packet.NewPacketReceiver("Aggregator 3")

	newPkts := packet.ChangePayload(pkt, []string{final})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: g.colaSalida,
		},
	}
}

// ============================ AggregatorQuery3 ===============================
