package global_aggregator

import (
	"bytes"
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

	sessions map[string](chan packet.Packet)
}

func (g *aggregator3Global) Build(rabbitAddr string) {
	g.colaEntrada = colas.InstanceQueue("PartialAggregations3", rabbitAddr)
	g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	//g.colaSalida = colas.InstanceQueue("GlobalAggregation3", rabbitAddr)
	g.acc = make(map[keyQuery3]float64)

	g.receiver = packet.NewPacketReceiver("Aggregator 3")

	g.sessions = make(map[string](chan packet.Packet))
}

// Función para procesar en una sesión específica - Query3
func processSessionQuery3(inputChannel chan packet.Packet, g *aggregator3Global) {
	localReceiver := packet.NewPacketReceiver("Agregador global 3 - Sesión")
	localAcc := make(map[keyQuery3]float64)

	for {
		pkt := <-inputChannel

		localReceiver.ReceivePacket(pkt)

		if !localReceiver.ReceivedAll() {
			continue
		}

		consolidatedInput := localReceiver.GetPayload()

		// Procesar el batch localmente
		lines := strings.Split(consolidatedInput, "\n")
		lines = lines[:len(lines)-1]

		for _, line := range lines {
			if line == "" {
				continue
			}
			cols := strings.Split(line, ",")
			if len(cols) != 3 {
				panic("Se esperaban 3 columnas")
			}
			yearHalf := cols[0]
			storeID := cols[1]
			totalStr := cols[2]

			total, err := strconv.ParseFloat(totalStr, 64)
			if err != nil {
				panic("Total con formato inválido")
			}

			k := keyQuery3{yearHalf: yearHalf, storeID: storeID}
			localAcc[k] += total
		}

		// Generar output y enviarlo
		if len(localAcc) == 0 {
			localReceiver = packet.NewPacketReceiver("Agregador global 3 - Sesión")
			continue
		}

		// En lugar de buscar el máximo, devolvemos todos los registros ordenados
		keys := make([]keyQuery3, 0, len(localAcc))
		for k := range localAcc {
			keys = append(keys, k)
		}

		// Ordenar por yearHalf y luego por storeID
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].yearHalf == keys[j].yearHalf {
				return keys[i].storeID < keys[j].storeID
			}
			return keys[i].yearHalf < keys[j].yearHalf
		})

		var b strings.Builder
		for _, k := range keys {
			total := localAcc[k]
			fmt.Fprintf(&b, "%s,%s,%.2f\n", k.yearHalf, k.storeID, total)
		}

		final := b.String()
		if final != "" {
			newPkts := packet.ChangePayloadGlobalAggregator(pkt, "transactions", []string{final})
			g.colaSalida.Send(newPkts[0].Serialize())
		}

		// Reset para la siguiente iteración
		localAcc = make(map[keyQuery3]float64)
		localReceiver = packet.NewPacketReceiver("Agregador global 3 - Sesión")
	}
}

func (g *aggregator3Global) PassPacketToSession(pkt packet.Packet) {
	slog.Info("Envio paquete a la sesión 3")
	sessionID := pkt.GetSessionID()
	channel, exists := g.sessions[sessionID]

	if !exists {
		slog.Info("Creo un hilo agregador 3 para nueva sesión")
		assigned_channel := make(chan packet.Packet)
		go processSessionQuery3(assigned_channel, g)

		g.sessions[sessionID] = assigned_channel
		channel = assigned_channel
	}

	channel <- pkt
}

func (g *aggregator3Global) Process() {
	slog.Info("Arranca procesamiento del agregador global 3 con session handling")

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
