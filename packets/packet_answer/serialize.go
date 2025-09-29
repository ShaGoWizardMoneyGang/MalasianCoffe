package packetanswer

import (
	"bytes"

	"malasian_coffe/protocol"
)

func (ah *AnswerHeader) serialize() ([]byte) {
	session_id_b := protocol.SerializeString(ah.session_id)
	query_b      := protocol.SerializeString(ah.query)

	answerheader := append(session_id_b, query_b...)

	return answerheader
}

func deserializeAnswerHeader(reader *bytes.Reader) (AnswerHeader, error){
	session_id, error  := protocol.DeserializeString(reader)
	if error != nil {
		return AnswerHeader{}, error
     }

	query, error := protocol.DeserializeString(reader)
	if error != nil {
		return AnswerHeader{}, error
     }

	header := AnswerHeader {
		session_id: session_id,
		query: query,
	}

	return header, nil
}

// ============================= Packet Answer =================================

func (pa *PacketAnswer) Serialize() ([]byte) {
	header_b     := pa.header.serialize()
	payload_b    := protocol.SerializeString(pa.payload)

	packetanswer := append(header_b, payload_b...)

	return packetanswer
}

func DeserializePackageAnswer(reader *bytes.Reader) (PacketAnswer, error) {
	header, error := deserializeAnswerHeader(reader)
	if error != nil {
		return PacketAnswer{}, error
     }
	// NOTE: En teoria, la data deberia estar consumida a este punto
	payload, error := protocol.DeserializeString(reader)
	if error != nil {
		return PacketAnswer{}, error
     }

	packet := PacketAnswer {
		header: header,
		payload: payload,
	}

	return packet, nil
}
