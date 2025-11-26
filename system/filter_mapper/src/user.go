package filter_mapper

import (
	"fmt"
	"log/slog"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	watchdog "malasian_coffe/system/watchdog/src"
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
	packet_channel chan colas.PacketMessage

	colaEntradaUsers *middleware.MessageMiddlewareQueue

	exchangeSalida4 *middleware.MessageMiddlewareExchange
}

func (ufm *userFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	ufm.packet_channel = make(chan colas.PacketMessage)

	ufm.colaEntradaUsers = colas.InstanceQueue("DataUsers", rabbitAddr)

	ufm.exchangeSalida4 = colas.InstanceExchange("FilteredUsers4", rabbitAddr, queueAmount["queue4"])
}

func (ufm *userFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return ufm.colaEntradaUsers
}

func (ufm *userFilterMapper) Process() {
	bitacora.Info("Arranca procesamiento de store filter mapper")

	go colas.InputQueue(ufm.colaEntradaUsers, ufm.packet_channel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-ufm.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()
			// Ambas payloads iguales
			filtered_users := []string{filterUsers(input)}

			newPayload := packet.ChangePayload(pkt, filtered_users)
			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: ufm.exchangeSalida4,
				},
			}

			for _, outbound := range outBoundMessages {
				cola := outbound.ColaSalida
				packet := outbound.Packet
				cola.Send(packet)
			}

			message.Ack(false)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Filter Users received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
