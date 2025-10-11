package concat

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"strings"
)

type Concat struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet

	colaInputTransaction *middleware.MessageMiddlewareQueue

	colaSalida  *middleware.MessageMiddlewareQueue

	sessionHandler sessionhandler.SessionHandler
}

func concat(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	localReceiver := packet.NewPacketReceiver("Concater")

	var concatenatedPackets strings.Builder

	var last_packet packet.Packet
	for {
		pkt := <-inputChannel

		localReceiver.ReceivePacket(pkt)

		if !localReceiver.ReceivedAll() {
			continue
		}

		concatenatedInput := localReceiver.GetPayload()
		concatenatedPackets.WriteString(concatenatedInput)
		last_packet = pkt
		break
	}


	pkt_joineado := packet.ChangePayloadGlobalAggregator(last_packet, "transactions", []string{concatenatedPackets.String()})

	// Liberamos
	concatenatedPackets.Reset()

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt concatenado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}
}

func (c *Concat) Build(rabbitAddr string) {
	c.inputChannel          = make(chan packet.Packet)

	c.outputChannel         = make(chan packet.Packet)

	c.colaInputTransaction  = colas.InstanceQueue("FilteredTransactions1", rabbitAddr)

	c.colaSalida            = colas.InstanceQueue("SalidaQuery1", rabbitAddr)

	c.sessionHandler        = sessionhandler.NewSessionHandler(concat, c.outputChannel)
}

func (c *Concat) Process() {
	go colas.InputQueue(c.colaInputTransaction, c.inputChannel)

	for {
		select {
		case inputPacket := <-c.inputChannel:
			c.sessionHandler.PassPacketToSession(inputPacket)
		case packetConcatenado := <-c.outputChannel:
			c.colaSalida.Send(packetConcatenado.Serialize())
		}
	}
}
