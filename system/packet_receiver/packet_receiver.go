package packetreceiver

import (
	"log/slog"
	"malasian_coffe/packets/packet"
)

type PacketReceiver struct {
	received_packages map[string]packet.Packet

	receivedEOF bool
}

func NewPacketReceiver() PacketReceiver {
	return PacketReceiver{
		received_packages: make(map[string]packet.Packet),
		receivedEOF: false,
	}
}

// Devuelve un booleano que representa si el packete se recibio en su
// totalidad o no
func (pr *PacketReceiver) ReceivePackt(pkt packet.Packet) {
	uuid := pkt.GetSessionID()

	_, exits := pr.received_packages[uuid]
	if exits {
		slog.Debug("Duplicate packet received")
	}
	pr.received_packages[uuid] = pkt
}
