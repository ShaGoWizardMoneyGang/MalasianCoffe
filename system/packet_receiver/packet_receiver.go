package packetreceiver

import (
	"log/slog"
	"malasian_coffe/packets/packet"
	"sort"
)

type PacketReceiver struct {
	received_packages map[string]packet.Packet

	// ordered_package []packet.Packet

	receivedEOF bool

	latestUUID string
}

func NewPacketReceiver() PacketReceiver {
	return PacketReceiver{
		received_packages: make(map[string]packet.Packet),
		receivedEOF: false,
		latestUUID: "",
	}
}

// Devuelve un booleano que representa si el packete se recibio en su
// totalidad o no
func (pr *PacketReceiver) ReceivePacket(pkt packet.Packet) {
	uuid := pkt.GetUUID()

	_, exits := pr.received_packages[uuid]
	if exits {
		slog.Debug("Duplicate packet received")
	}

	pr.received_packages[uuid] = pkt

	if !pr.receivedEOF {
		pr.receivedEOF = pkt.IsEOF()
	}
}

func (pr *PacketReceiver) GetPackets() bool {
	if !pr.receivedEOF {
		return false
	}

	packets := make([]packet.Packet, len(pr.received_packages))

	i := 0
	for _, packet := range pr.received_packages {
		packets[i] = packet
		i += 1
	}
	sort.Slice(packets, func(i, j int) bool {
		return packets[i].GetSessionID() > packets[j].GetSessionID()
	})


}
