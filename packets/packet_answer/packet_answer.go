package packetanswer

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
func From (packet Packet, query string) PacketAnswer {
	payload      := packet.GetPayload()
	session_id   := packet.header.session_id
	packetanswer := PacketAnswer {
		header: AnswerHeader {
			session_id: session_id,
			query: query,
		},
		payload: payload,
	}

	return packetanswer
}
