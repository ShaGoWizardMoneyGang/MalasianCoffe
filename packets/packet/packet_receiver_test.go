package packet

import (
	"testing"
)

func TestPacketReceiverInOrder(t *testing.T) {
	s_id := "testing-session"
	// El ultimo es 13, porque si no anda, mala suerte
	shuffled_packets := []Packet{
		{
			header: newHeader(s_id, newPacketUuid("0.0", false , false), "localhost:9091"),
			payload: "00",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.4", false, true), "localhost:9091"),
			payload: "04",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.1", false, false), "localhost:9091"),
			payload: "01",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.3", false, false), "localhost:9091"),
			payload: "03",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.2", false, false), "localhost:9091"),
			payload: "02",
		},
	}

	packet_receiver := NewPacketReceiver()
	for _, packet := range shuffled_packets {
		packet_receiver.ReceivePacket(packet)
	}

	_, is_ordered, err := packet_receiver.GetPackets()
	if err != nil {
		t.Fatal(err)
	}
	if is_ordered != true {
		panic("Packet receiver failed to recognize that packets are ordered.")
	}
}

func TestPacketReceiverNotInOrder(t *testing.T) {
	s_id := "testing-session"
	// El ultimo es 13, porque si no anda, mala suerte
	shuffled_packets := []Packet{
		{
			header: newHeader(s_id, newPacketUuid("0.0", false , false), "localhost:9091"),
			payload: "00",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.1", false, false), "localhost:9091"),
			payload: "01",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.3", false, false), "localhost:9091"),
			payload: "03",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.2", false, false), "localhost:9091"),
			payload: "02",
		},
	}

	packet_receiver := NewPacketReceiver()
	for _, packet := range shuffled_packets {
		packet_receiver.ReceivePacket(packet)
	}

	_, is_ordered, err := packet_receiver.GetPackets()
	if err != nil {
		t.Fatal(err)
	}
	if is_ordered != false {
		panic("Packet receiver failed to recognize that packets are not ordered.")
	}
}
