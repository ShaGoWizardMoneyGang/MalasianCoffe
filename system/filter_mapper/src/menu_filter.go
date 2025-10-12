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

	exchangeSalida2a *middleware.MessageMiddlewareExchange
	exchangeSalida2b *middleware.MessageMiddlewareExchange
}

func (mifm *menuItemFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	colaEntradaStore := colas.InstanceQueue("DataMenuItems", rabbitAddr)

	mifm.exchangeSalida2a = colas.InstanceExchange("FilteredMenuItems2a", rabbitAddr, queueAmount["queue2a"])
	mifm.exchangeSalida2b = colas.InstanceExchange("FilteredMenuItems2b", rabbitAddr, queueAmount["queue2b"])

	mifm.colaEntradaStore = colaEntradaStore

}

func (mifm *menuItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return mifm.colaEntradaStore
}

func (mifm *menuItemFilterMapper) Process(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	mapped_stores := filterMenus(input)
	newPayload := packet.ChangePayload(pkt, mapped_stores)
	outBoundMessage := []colas.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.exchangeSalida2a,
		},
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.exchangeSalida2b,
		},
	}

	return outBoundMessage
}
