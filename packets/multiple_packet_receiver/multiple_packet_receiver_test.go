package multiple_packet_receiver

import (
	"fmt"
	"testing"
)

func TestPacketReceiverInOrder(t *testing.T) {
	s_id := "testing-session"
	shuffled_packets := []Packet{
		{
			header: newHeader(s_id, newPacketUuid("0.0", false), "localhost:9091"),
			payload: "00",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.4", true), "localhost:9091"),
			payload: "04",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.1", false), "localhost:9091"),
			payload: "01",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.3", false), "localhost:9091"),
			payload: "03",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.2", false), "localhost:9091"),
			payload: "02",
		},
	}

	packet_receiver := NewSinglePacketReceiver("Packet receiver testing")
	for _, packet := range shuffled_packets {
		packet_receiver.ReceivePacket(packet)
	}

	is_ordered := packet_receiver.ReceivedAll()
	if is_ordered != true {
		panic("Packet receiver failed to recognize that packets are ordered.")
	}

	expected_payload := "0001020304"
	accum_payload  := packet_receiver.GetPayload()
	if accum_payload != expected_payload {
		panic(fmt.Errorf(`No se obtuvo el payload esperado:
Esperaba %s
Obtuve %s
`, expected_payload, accum_payload))
	}
}

func TestPacketReceiverNotInOrder(t *testing.T) {
	s_id := "testing-session"
	// Le falta el paquete 0.1
	shuffled_packets := []Packet{
		{
			header: newHeader(s_id, newPacketUuid("0.0", false), "localhost:9091"),
			payload: "00",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.3", true), "localhost:9091"),
			payload: "03",
		},
		{
			header: newHeader(s_id, newPacketUuid("0.2", false), "localhost:9091"),
			payload: "02",
		},
	}

	packet_receiver := NewSinglePacketReceiver("Packet receiver not in order test")
	for _, packet := range shuffled_packets {
		packet_receiver.ReceivePacket(packet)
	}

	is_ordered := packet_receiver.ReceivedAll()
	if is_ordered != false {
		panic("Packet receiver failed to recognize that packets are not ordered.")
	}
}
