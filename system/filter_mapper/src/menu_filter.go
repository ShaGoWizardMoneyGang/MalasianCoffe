package filter_mapper

import (
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strings"
)

func filterMenus(input string) []string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 6 {
			panic("Invalid data format")
		}

		if data[0] == "" ||
			data[1] == "" {
			bitacora.Debug("Registro con campos de interes vacios, dropeado")
			continue
		}
		final += data[0] + "," + data[1] + "\n"
	}
	return []string{final, final}
}

type menuItemFilterMapper struct {
	colaEntradaStore *middleware.MessageMiddlewareQueue

	colaSalida2a *middleware.MessageMiddlewareQueue
	colaSalida2b *middleware.MessageMiddlewareQueue
}

func (mifm *menuItemFilterMapper) Build(rabbitAddr string) {
	colaEntradaStore := colas.InstanceQueue("DataMenuItems", rabbitAddr)

	colaSalida2a := colas.InstanceQueue("FilteredMenuItems2a", rabbitAddr)
	colaSalida2b := colas.InstanceQueue("FilteredMenuItems2b", rabbitAddr)

	mifm.colaEntradaStore = colaEntradaStore

	mifm.colaSalida2a = colaSalida2a
	mifm.colaSalida2b = colaSalida2b

}

func (mifm *menuItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return mifm.colaEntradaStore
}

func (mifm *menuItemFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	mapped_stores := filterMenus(input)
	newPayload := packet.ChangePayload(pkt, mapped_stores)
	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.colaSalida2a,
		},
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.colaSalida2b,
		},
	}

	return outBoundMessage
}
