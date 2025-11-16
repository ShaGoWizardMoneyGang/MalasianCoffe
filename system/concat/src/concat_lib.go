package concat

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/single_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
)

type Concat struct {
	inputChannel chan colas.PacketMessage

	outputChannel chan packet.Packet

	colaInputTransaction *middleware.MessageMiddlewareQueue

	colaSalida *middleware.MessageMiddlewareQueue

	sessionHandler sessionhandler.SessionHandler
}

func concat_func(accumulated_input string, new_input string) string {
	concatenated := accumulated_input + new_input
	return concatenated
}

func concat(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	localReceiver := single_packet_receiver.NewSinglePacketReceiver(sessionID, concat_func)

	var concatenated_packets string
	var last_packet packet.Packet
	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		received_all := localReceiver.ReceivePacket(pktMsg)

		if !received_all {
			continue
		}

		concatenated_packets = localReceiver.GetPayload()
		last_packet = pkt
		break
	}

	pkt_joineado := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{concatenated_packets})

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt concatenado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}

	localReceiver.Clean()
}

func (c *Concat) Build(rabbitAddr string, routing_key string) {
	c.inputChannel = make(chan colas.PacketMessage)

	c.outputChannel = make(chan packet.Packet)

	c.colaInputTransaction = colas.InstanceQueueRouted("FilteredTransactions1", rabbitAddr, routing_key)

	c.colaSalida = colas.InstanceQueue("SalidaQuery1", rabbitAddr)

	c.sessionHandler = sessionhandler.NewSessionHandler(concat, c.outputChannel)
}

func (c *Concat) Process() {
	go colas.InputQueue(c.colaInputTransaction, c.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-c.inputChannel:
			c.sessionHandler.PassPacketToSession(inputPacket)
		case packetConcatenado := <-c.outputChannel:
			c.colaSalida.Send(packetConcatenado)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Concat received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
