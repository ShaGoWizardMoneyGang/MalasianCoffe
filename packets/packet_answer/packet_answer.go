package packetanswer

import (
	"malasian_coffe/packets/packet"
)

// Este es el packet que se envia como respuesta a la query. Difiere del input.
type AnswerHeader struct {
	// ID de la session a la que este paquete corresponde
	// Usado como sanity check
	session_id string

	// Representa a que query pertenece la respuesta
	query string
}

type PacketAnswer struct {
	header AnswerHeader
	payload string
}

// Crea un PacketAnswer de un Packet
func From (packet packet.Packet, query string) PacketAnswer {
	payload      := packet.GetPayload()
	session_id   := packet.GetSessionID()
	packetanswer := PacketAnswer {
		header: AnswerHeader {
			session_id: session_id,
			query: query,
		},
		payload: payload,
	}

	return packetanswer
}

func (p* PacketAnswer) GetQuery() string {
	return p.header.query
}

func (p* PacketAnswer) GetPayload() string {
	return p.payload
}

func (p* PacketAnswer) GetSessionID() string {
	return p.header.session_id
}
