package sessionhandler

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/colas"
)

type SessionHandler struct {
	outputChannel chan<- packet.Packet

	sessionMap map[string](chan colas.PacketMessage)

	associatedFunction func(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet)
}

func NewSessionHandler(sessionFunction func(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet),
	outputChannel chan<- packet.Packet) SessionHandler {

	// Map from SessionID to session channel
	sessionMap := make(map[string](chan colas.PacketMessage))

	return SessionHandler{
		sessionMap:         sessionMap,
		associatedFunction: sessionFunction,
		outputChannel:      outputChannel,
	}
}

// Funcion que le pasa el packet.Packet a una session.
func (sh *SessionHandler) PassPacketToSession(pktMsg colas.PacketMessage) {
	pkt := pktMsg.Packet
	sessionID := pkt.GetSessionID()
	channel, exists := sh.sessionMap[sessionID]

	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		bitacora.Info(fmt.Sprintf("Nueva session detectada: %s", sessionID))
		// Canal de input asignado a esta session
		assigned_channel := make(chan colas.PacketMessage)
		go sh.associatedFunction(assigned_channel, sh.outputChannel)

		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		sh.sessionMap[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pktMsg //pushea al output channel (inferido porque no sabemos como funca)
}
