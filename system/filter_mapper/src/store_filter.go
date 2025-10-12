package filter_mapper

import (
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strings"
)

func filterStores(input string) []string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 8 {
			panic("Invalid data format")
		}

		if data[0] == "" ||
			data[1] == "" {
			slog.Debug("Registro con campos de interes vacios, dropeado")
			continue
		}
		final += data[0] + "," + data[1] + "\n"
	}
	return []string{final, final}
}

type storeFilterMapper struct {
	colaEntradaStore *middleware.MessageMiddlewareQueue

	exchangeSalida3 *middleware.MessageMiddlewareExchange
	exchangeSalida4 *middleware.MessageMiddlewareExchange
}

func (sfm *storeFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	colaEntradaStore := colas.InstanceQueue("DataStores", rabbitAddr)

	// colaSalida3 := colas.InstanceQueue("FilteredStores3", rabbitAddr)

	sfm.colaEntradaStore = colaEntradaStore

	sfm.exchangeSalida3 = colas.InstanceExchange("FilteredStores3", rabbitAddr, queueAmount["queue3"])
	sfm.exchangeSalida4 = colas.InstanceExchange("FilteredStores4", rabbitAddr, queueAmount["queue4"])
}

func (sfm *storeFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return sfm.colaEntradaStore
}

func (sfm *storeFilterMapper) Process(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	mapped_stores := filterStores(input)
	newPayload := packet.ChangePayload(pkt, mapped_stores)
	outBoundMessage := []colas.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: sfm.exchangeSalida3,
		},
		{
			Packet:     newPayload[1],
			ColaSalida: sfm.exchangeSalida4,
		},
	}

	return outBoundMessage
}
