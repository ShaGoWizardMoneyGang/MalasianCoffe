package packet

import (
	"bytes"
	"fmt"
	"testing"
)


func TestPacketSerialization(t *testing.T) {
	packet_uuid := packetUuid {
		"0.0",
		false,
	};

	header      := newHeader("session-id", packet_uuid, "localhost:9091")
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

func TestNewPayloadSplit(t *testing.T) {
	packet_uuid := newPacketUuid("0.0", true)
	header      := newHeader("session-id", packet_uuid, "localhost:9091")
	payloads    := []string{"12345", "12345", "123"}
	var payloadEnsemble string
	for _, payload := range payloads {
		payloadEnsemble += payload
	}

	packet      := Packet {
		header:  header,
		payload: payloadEnsemble,
	}

	packets     := newPayloads(packet, payloadEnsemble, 5)
	if len(packets) != 3 {
		panic("Failed to split packet into 3")
	}

	for i, packet := range packets {
		if packet.GetPayload() != payloads[i] {
			panic(fmt.Sprintf(`Packet does not contain expected payload in position %d,
Got %s
Expected %s`,i ,packet.GetPayload(), payloads[i]))
		}

		fmt.Printf("%v\n", packet)
		if i == len(packets) - 1 {
			if !packets[i].IsEOF() {
				panic(fmt.Sprintf("Packet is not marked as EOF even though it should %v", packet))
			}
		} else {
			if packets[i].IsEOF() {
				panic(fmt.Sprintf("Packet is marked as EOF even though it shouldn't %v", packet))
			}
		}
	}
}

