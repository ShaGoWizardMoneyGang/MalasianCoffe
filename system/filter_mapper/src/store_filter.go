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

	colaSalida3 *middleware.MessageMiddlewareQueue
	colaSalida4 *middleware.MessageMiddlewareQueue
}

func (sfm *storeFilterMapper) Build(rabbitAddr string) {
	colaEntradaStore := colas.InstanceQueue("DataStores", rabbitAddr)

	colaSalida3 := colas.InstanceQueue("FilteredStores3", rabbitAddr)

	sfm.colaEntradaStore = colaEntradaStore

	sfm.colaSalida3      = colaSalida3
	// sfm.exchangeSalida4  = colas.InstanceExchange("FilteredStores4", rabbitAddr)
	sfm.colaSalida4 = colas.InstanceQueue("FilteredStores4", rabbitAddr)
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
			ColaSalida: sfm.colaSalida3,
		},
		{
			Packet:     newPayload[1],
			ColaSalida: sfm.colaSalida4,
		},
	}

	return outBoundMessage
}
