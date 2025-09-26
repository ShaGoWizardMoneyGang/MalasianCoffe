package packet

import (
	"bytes"
	"fmt"
	"testing"
)


func TestPacketSerialization(t *testing.T) {
	packet_uuid := PacketUuid {
		"0.0",
		false,
	};

	header      := newHeader(23, packet_uuid, "localhost:9091")
	payload     := "extra territorium jus dicenti impune non paretur"

	packet      := Packet {
		header: header,
		payload: payload,
	}

	packet_serialized := packet.Serialize()
	reader := bytes.NewReader(packet_serialized)
	packet_deserialized, err := DeserializePackage(reader)
	if err != nil {
		t.Fatal(err)
	}

	if packet != packet_deserialized {
		fmt.Printf("Before serialization: %v\n", packet)
		fmt.Printf("After serialization: %v\n", packet_deserialized)
		t.Fatal("ERROR: Packets differ")
	}
}
