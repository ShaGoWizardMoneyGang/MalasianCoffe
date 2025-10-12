package filter_mapper

import (
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strings"
)

func filterUsers(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 4 {
			panic("Invalid data format")
		}
		if data[0] == "" || data[2] == "" {
			slog.Debug("Registro con menos de 9 columnas, dropeado")
			continue
		}
		final += data[0] + "," + data[2] + "\n"
	}
	return final
}


type userFilterMapper struct {
	colaEntradaUsers *middleware.MessageMiddlewareQueue

	exchangeSalida4 *middleware.MessageMiddlewareExchange
}

func (ufm *userFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	colaEntradaUsers := colas.InstanceQueue("DataUsers", rabbitAddr)

	ufm.colaEntradaUsers = colaEntradaUsers

	ufm.exchangeSalida4 = colas.InstanceExchange("FilteredUsers4", rabbitAddr, queueAmount["queue4"])
}

func (ufm *userFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return ufm.colaEntradaUsers
}

func (ufm *userFilterMapper) Process(pkt packet.Packet) []colas.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	filtered_users := []string{filterUsers(input)}

	newPayload := packet.ChangePayload(pkt, filtered_users)
	outBoundMessage := []colas.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: ufm.exchangeSalida4,
		},
	}

	return outBoundMessage
}
