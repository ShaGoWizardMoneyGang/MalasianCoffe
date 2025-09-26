package packet

import (
	"fmt"
	"testing"
)


func TestPacketSerialization(t *testing.T) {
	packet_uuid := PacketUuid {
		"0.0",
		false,
	};

	header      := newHeader(23, packet_uuid, "localhost:9091")
	payload     := []byte("extra territorium jus dicenti impune non paretur")

	packet      := Packet {
		header: header,
		payload: payload,
	}
	fmt.Printf("{%v}\n", packet)

	packet_serialized := packet.Serialize()
	packet_deserialized, err := DeserializePackage(&packet_serialized)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("{%v}\n", packet)
	fmt.Printf("{%v}\n", packet_deserialized)
}
