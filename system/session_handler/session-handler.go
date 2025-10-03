package sessionhandler

import "malasian_coffe/packets/packet"

type SessionHandler struct {
	outputChannel      chan<- packet.Packet

	sessionMap         map[string](chan packet.Packet)

	associatedFunction func(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) ()
}

func NewSessionHandler(sessionFunction func(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) (), outputChannel chan<- packet.Packet) SessionHandler {
	// Map from SessionID to session channel
	sessionMap := make(map[string](chan packet.Packet))

	return SessionHandler{
		sessionMap: sessionMap,
		associatedFunction: sessionFunction,
		outputChannel: outputChannel,
	}
}

// Funcion que le pasa el packet.Packet a una session.
func (sh *SessionHandler) PassPacketToSession(pkt packet.Packet) {
	sessionID := pkt.GetSessionID()
	channel, exists := sh.sessionMap[sessionID]

	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		// Canal de input asignado a esta session
		assigned_channel := make(chan packet.Packet)
		go sh.associatedFunction(assigned_channel, sh.outputChannel)

		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		sh.sessionMap[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pkt
}
