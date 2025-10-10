package global_aggregator

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"sort"
	"strconv"
	"strings"
)

// AggregatorGLobal de la query2b ES LA SUBTOTAL
type keyQuery2b struct {
	yearMonth string
	itemID    string
}

type aggregator2bGlobal struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery2b]float64

	receiver packet.PacketReceiver

	sessions map[string](chan packet.Packet)
}

func (g *aggregator2bGlobal) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("CountedItems2b", rabbitAddr)
	// aca va GlobalAggregation2b
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2b", rabbitAddr)
	g.acc = make(map[keyQuery2b]float64)

	g.receiver = packet.NewPacketReceiver("Aggregator 2b")

	g.sessions = make(map[string](chan packet.Packet))
}

func processSessionQuery2b(inputChannel chan packet.Packet, g *aggregator2bGlobal) {
	localReceiver := packet.NewPacketReceiver("Agregador global 2b - Sesión")
	localAcc := make(map[keyQuery2b]float64)

	for {
		pkt := <-inputChannel

		localReceiver.ReceivePacket(pkt)

		if !localReceiver.ReceivedAll() {
			continue
		}

		consolidatedInput := localReceiver.GetPayload()

		lines := strings.Split(consolidatedInput, "\n")
		lines = lines[:len(lines)-1]

		for _, line := range lines {
			if line == "" {
				continue
			}
			cols := strings.Split(line, ",")
			if len(cols) != 3 {
				bitacora.Debug("Se esperaban 3 columnas")
				continue
			}
			yearMonth := cols[0]
			itemID := cols[1]
			subtotalStr := cols[2]

			subtotal, err := strconv.ParseFloat(subtotalStr, 64)
			if err != nil {
				panic("Subtotal con formato inválido")
			}

			k := keyQuery2b{yearMonth: yearMonth, itemID: itemID}
			localAcc[k] += subtotal
		}

		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global 2b - Sesión")
			continue
		}

		monthlyMax := make(map[string]struct {
			itemID   string
			subtotal float64
		})

		for k, value := range localAcc {
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

		final := b.String()
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transaction_items", []string{final})
			g.colaSalida.Send(newPkts[0].Serialize())
		}

		localAcc = make(map[keyQuery2b]float64)
		localReceiver = packet.NewPacketReceiver("Agregador global 2b - Sesión")
	}
}

func (g *aggregator2bGlobal) PassPacketToSession(pkt packet.Packet) {
	sessionID := pkt.GetSessionID()
	channel, exists := g.sessions[sessionID]

	if !exists {
		slog.Info("Creo un hilo agregador 2b para nueva sesión")
		assigned_channel := make(chan packet.Packet)
		go processSessionQuery2b(assigned_channel, g)

		g.sessions[sessionID] = assigned_channel
		channel = assigned_channel
	}

	channel <- pkt
}

func (g *aggregator2bGlobal) Process() {
	slog.Info("Arranca procesamiento del agregador global 2b con session handling")

	msgQueue := colas.ConsumeInput(g.colaEntrada)

	for message := range *msgQueue {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}

		g.PassPacketToSession(pkt)
	}
}
