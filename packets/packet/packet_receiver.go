package packet

import (
	"fmt"
	"log/slog"
	"sort"
)

type PacketReceiver struct {
	received_packages map[string]Packet

	// ordered_package []packet.Packet

	receivedEOF bool

}

func NewPacketReceiver() PacketReceiver {
	return PacketReceiver{
		received_packages: make(map[string]Packet),
		receivedEOF: false,
	}
}

// Devuelve un booleano que representa si el packete se recibio en su
// totalidad o no
func (pr *PacketReceiver) ReceivePacket(pkt Packet) {
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

func (pr *PacketReceiver) GetPackets() ([]Packet, bool, error) {
	if !pr.receivedEOF {
		return []Packet{}, false, nil
	}

	packets := make([]Packet, len(pr.received_packages))
	w := 0
	for _, packet := range pr.received_packages {
		packets[w] = packet
		w += 1
	}

	{
		sort.Slice(packets, func(i, j int) bool {
			pkt_i, _ := packets[i].GetSequenceNumber()
			pkt_j, _ := packets[j].GetSequenceNumber()
			return  pkt_i < pkt_j
		})
	}

	ordered := true
	for i, pkt := range packets {
		// Llegue al ultimo packet, tiene que ser el EOF
		if i == len(packets) - 1 {
			ordered = pkt.IsEOF()
			break
		}

		nxt_pkt         := packets[i + 1]
		nxt_pkt_sn, err := nxt_pkt.GetSequenceNumber()
		if err != nil {
			return []Packet{}, false, fmt.Errorf("Failed to get sequence number, %w", err)
		}

		pkt_sn, err     := pkt.GetSequenceNumber()
		if err != nil {
			return []Packet{}, false, fmt.Errorf("Failed to get sequence number, %w", err)
		}

		if pkt_sn + 1 != nxt_pkt_sn {
			ordered = false
			break
		}
	}

	return packets, ordered, nil

}
