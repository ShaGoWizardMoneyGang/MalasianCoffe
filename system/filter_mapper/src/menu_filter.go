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
	packet_channel chan colas.PacketMessage

	colaEntradaStore *middleware.MessageMiddlewareQueue

	exchangeSalida2a *middleware.MessageMiddlewareExchange
	exchangeSalida2b *middleware.MessageMiddlewareExchange
}

func (mifm *menuItemFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	mifm.packet_channel = make(chan colas.PacketMessage)

	mifm.exchangeSalida2a = colas.InstanceExchange("FilteredMenuItems2a", rabbitAddr, queueAmount["queue2a"])
	mifm.exchangeSalida2b = colas.InstanceExchange("FilteredMenuItems2b", rabbitAddr, queueAmount["queue2b"])

	mifm.colaEntradaStore = colas.InstanceQueue("DataMenuItems", rabbitAddr)

}

func (mifm *menuItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return mifm.colaEntradaStore
}

func (mifm *menuItemFilterMapper) Process() {
	slog.Info("Arranca procesamiento de store filter mapper")

	go colas.InputQueue(mifm.colaEntradaStore, mifm.packet_channel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-mifm.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()

			mapped_stores := filterMenus(input)
			newPayload := packet.ChangePayload(pkt, mapped_stores)
			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: mifm.exchangeSalida2a,
				},
				{
					Packet:     newPayload[0],
					ColaSalida: mifm.exchangeSalida2b,
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
			fmt.Println("Filter MenuItems received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
