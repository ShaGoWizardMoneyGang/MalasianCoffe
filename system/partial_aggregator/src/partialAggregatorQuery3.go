package partial_aggregator

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
	"sort"
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

	lines := strings.SplitSeq(input, "\n")
	for line := range lines {
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

	// ordeno por semestre y después por store_id
	keys := make([]key, 0, len(acc))
	for k := range acc {
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
		// NOTA FABRI: Esto me tiraba conflicto, dejo ambos.
		// value := strconv.FormatFloat(acc[k], 'f', 0, 64)
		value := strconv.FormatFloat(acc[k], 'f', 1, 64)

		fmt.Fprintf(&b, "%s,%s,%s\n", k.yearHalf, k.storeID, value)
	}
	return b.String()
}

type PartialAggregator interface {
	Build(rabbitAddr string, outs map[string]uint64)
	GetInput() *middleware.MessageMiddlewareQueue
	Process()
}

type aggregator3Partial struct {
	packet_channel chan colas.PacketMessage
	colaEntrada    *middleware.MessageMiddlewareQueue
	exchangeSalida *middleware.MessageMiddlewareExchange
}

func (a *aggregator3Partial) Build(rabbitAddr string, outs map[string]uint64) {
	// mismas colas que las de antes
	a.packet_channel = make(chan colas.PacketMessage)
	a.colaEntrada = colas.InstanceQueue("FilteredTransactions3", rabbitAddr)
	a.exchangeSalida = colas.InstanceExchange("PartialAggregations3", rabbitAddr, outs["queue"])
}

func (a *aggregator3Partial) GetInput() *middleware.MessageMiddlewareQueue {
	return a.colaEntrada
}

func (a *aggregator3Partial) processPacket(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()
	slog.Debug("Aggregator3Partial.Process: recibí payload")

	result := aggregator3BySemesterTPV(input)

	newPkts := packet.ChangePayload(pkt, []string{result})
	return []colas.OutBoundMessage{
		{
			Packet:     newPkts[0],
			ColaSalida: a.exchangeSalida,
		},
	}
}

func (a *aggregator3Partial) Process() {
	slog.Info("Arranca procesamiento de aggregator3Partial con watchdog")

	go colas.InputQueue(a.colaEntrada, a.packet_channel)

	watchdogListener := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdogListener.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-a.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			outboundMessages := a.processPacket(pkt)
			for _, outbound := range outboundMessages {
				cola := outbound.ColaSalida
				p := outbound.Packet
				if err := cola.Send(p); err != 0 {
					slog.Error("Error enviando a cola de salida", "err", err)
				}
			}
			if err := message.Ack(false); err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("PartialAggregator3 received healthcheck ping from", IP)
			watchdogListener.Pong(IP)
		}
	}
}
