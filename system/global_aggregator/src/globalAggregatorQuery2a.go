package global_aggregator

import (
	"bytes"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
)

// AggregatorGLobal de la query2a ES LA QUANTITY
type keyQuery2a struct {
	yearMonth string
	itemID    string
}

type aggregator2aGlobal struct {
	colaEntrada *middleware.MessageMiddlewareQueue
	colaSalida  *middleware.MessageMiddlewareQueue
	acc         map[keyQuery2a]int64

	receiver packet.PacketReceiver
	sessions map[string](chan packet.Packet)
}

func (g *aggregator2aGlobal) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("CountedItems2a", rabbitAddr)
	// aca va GlobalAggregation2a
	g.colaSalida = colas.InstanceQueue("GlobalAggregation2a", rabbitAddr)
	g.acc = make(map[keyQuery2a]int64)

	g.receiver = packet.NewPacketReceiver("Agregador global")
	g.sessions = make(map[string](chan packet.Packet))
}

func processSessionQuery2a(inputChannel chan packet.Packet, g *aggregator2aGlobal) {
	localReceiver := packet.NewPacketReceiver("Agregador global - Sesión")
	localAcc := make(map[keyQuery2a]int64)

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
			quantityStr := cols[2]

			quantity, err := strconv.ParseInt(quantityStr, 10, 64)
			if err != nil {
				panic("Quantity con formato inválido")
			}

			k := keyQuery2a{yearMonth: yearMonth, itemID: itemID}
			localAcc[k] += quantity
		}

		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global - Sesión")
			continue
		}

		monthlyMax := make(map[string]struct {
			itemID   string
			quantity int64
		})

		for k, value := range localAcc {
			yearMonth := k.yearMonth
			if current, exists := monthlyMax[yearMonth]; !exists || value > current.quantity {
				monthlyMax[yearMonth] = struct {
					itemID   string
					quantity int64
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
			fmt.Fprintf(&b, "%s,%s,%d\n", month, maxItem.itemID, maxItem.quantity)
		}

		final := b.String()
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transaction_items", []string{final})
			g.colaSalida.Send(newPkts[0].Serialize())
		}

		localAcc = make(map[keyQuery2a]int64)
		localReceiver = packet.NewPacketReceiver("Agregador global - Sesión")
	}
}

func (g *aggregator2aGlobal) PassPacketToSession(pkt packet.Packet) {
	sessionID := pkt.GetSessionID()
	channel, exists := g.sessions[sessionID]

	if !exists {
		slog.Info("Creo un hilo agregador para nueva sesión")
		assigned_channel := make(chan packet.Packet)
		go processSessionQuery2a(assigned_channel, g)

		g.sessions[sessionID] = assigned_channel
		channel = assigned_channel
	}

	channel <- pkt
}
func (g *aggregator2aGlobal) Process() {
	slog.Info("Arranca procesamiento del agregador global 2a con session handling")

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
