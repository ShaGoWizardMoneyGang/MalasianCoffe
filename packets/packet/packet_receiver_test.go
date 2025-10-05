package packet

import (
	"math/rand"
	"testing"
)

func TestPacketReceiver(t *testing.T) {
	s_id := "testing-session"
	// El ultimo es 13, porque si no anda, mala suerte
	packets := []Packet{
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.0", eof: false}, "localhost:9091"),
			payload: "00",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.1", eof: false}, "localhost:9091"),
			payload: "01",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.2", eof: false}, "localhost:9091"),
			payload: "02",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.3", eof: false}, "localhost:9091"),
			payload: "03",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.4", eof: false}, "localhost:9091"),
			payload: "04",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.5", eof: false}, "localhost:9091"),
			payload: "05",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.6", eof: false}, "localhost:9091"),
			payload: "06",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.7", eof: false}, "localhost:9091"),
			payload: "07",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.8.0", eof: false}, "localhost:9091"),
			payload: "080",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.8.1", eof: false}, "localhost:9091"),
			payload: "081",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.8.2", eof: false}, "localhost:9091"),
			payload: "082",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.8.3", eof: false}, "localhost:9091"),
			payload: "083",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.8.4", eof: false}, "localhost:9091"),
			payload: "084",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.9", eof: false}, "localhost:9091"),
			payload: "09",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.10", eof: false}, "localhost:9091"),
			payload: "010",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.0", eof: false}, "localhost:9091"),
			payload: "0110",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.1", eof: false}, "localhost:9091"),
			payload: "0111",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.2", eof: false}, "localhost:9091"),
			payload: "0112",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.3", eof: false}, "localhost:9091"),
			payload: "0113",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.4", eof: false}, "localhost:9091"),
			payload: "0114",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.11.5", eof: false}, "localhost:9091"),
			payload: "0115",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.12", eof: false}, "localhost:9091"),
			payload: "012",
		},
		{
			header: newHeader(s_id, packetUuid{ uuid: "0.13", eof: true}, "localhost:9091"),
			payload: "013",
		},
	}

	shuffled_packets := make([]Packet, len(packets))
	// Mezcla fisher yates
	for i := range packets {
		j := rand.Intn(i + 1)
		shuffled_packets[i], shuffled_packets[j] = shuffled_packets[j], shuffled_packets[i]
	}

	packet_receiver := NewPacketReceiver()
	for _, packet := range shuffled_packets {
		packet_receiver.ReceivePacket(packet)
	}
}
