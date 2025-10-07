package partial_aggregator

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strconv"
	"strings"
	"time"
)

// Recibe: "store_id,final_amount,created_at" por línea
// Devuelve: "YYYY-H{1|2},store_id,tpv" ordenado por semestre y luego store_id
func aggregator3BySemesterTPV(input string) string {
	const layout = "2006-01-02 15:04:05"

	type key struct {
		yearHalf string
		storeID  string
	}
	acc := make(map[key]float64)

	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 3 {
			panic("Se esperaban 3 columnas: store_id,final_amount,created_at")
		}

		storeID := cols[0]

		amount, err := strconv.ParseFloat(cols[1], 64)
		if err != nil {
			panic(fmt.Sprintf("final_amount con formato inválido: %s", cols[1]))
		}

		ts, err := time.Parse(layout, cols[2])
		if err != nil {
			panic("created_at con formato inválido")
		}

		half := "H1"
		if ts.Month() >= 7 {
			half = "H2"
		}
		yh := fmt.Sprintf("%04d-%s", ts.Year(), half)

		k := key{yearHalf: yh, storeID: storeID}
		acc[k] += amount
	}

	var b strings.Builder
	for key, accum := range acc {
		// NOTA FABRI: Esto me tiraba conflicto, dejo ambos.
		// value := strconv.FormatFloat(acc[k], 'f', 0, 64)
		value := strconv.FormatFloat(accum, 'f', 1, 64)

		fmt.Fprintf(&b, "%s,%s,%s\n", key.yearHalf, key.storeID, value)
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

	receiver packet.PacketReceiver
}

func (a *aggregator3Partial) Build(rabbitAddr string) {
	// mismas colas que las de antes
	a.colaEntrada = colas.InstanceQueue("FilteredTransactions3", rabbitAddr)
	a.colaSalida = colas.InstanceQueue("PartialAggregations3", rabbitAddr)

	a.receiver = packet.NewPacketReceiver()
}

func (a *aggregator3Partial) GetInput() *middleware.MessageMiddlewareQueue {
	return a.colaEntrada
}

func (a *aggregator3Partial) Process(pkt packet.Packet) []packet.OutBoundMessage {
	a.receiver.ReceivePacket(pkt)

	if !a.receiver.ReceivedAll() {
		slog.Debug("Aún no se han recibido todos los paquetes")
		return nil
	}

	consolidatedInput := a.receiver.GetPayload()

	result := aggregator3BySemesterTPV(consolidatedInput)

	a.receiver = packet.NewPacketReceiver()

	newPkts := packet.ChangePayload(pkt, []string{result})
	return []packet.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: a.colaSalida,
		},
	}
}
