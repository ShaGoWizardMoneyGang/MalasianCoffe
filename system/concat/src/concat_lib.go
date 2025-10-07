package concat

import (
	"malasian_coffe/packets/packet"
)

type Concat struct {
	receiver packet.PacketReceiver
}

func NewConcat() *Concat {
	return &Concat{
		receiver: packet.NewPacketReceiver(),
	}
}

func (c *Concat) Process(pkt packet.Packet) []packet.Packet {
	c.receiver.ReceivePacket(pkt)

	if !c.receiver.ReceivedAll() {
		return []packet.Packet{}
	}

	consolidatedInput := c.receiver.GetPayload()

	outputs := []string{consolidatedInput}
	newPacket := packet.ChangePayload(pkt, outputs)

	c.receiver = packet.NewPacketReceiver()

	return newPacket
}
