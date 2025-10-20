package packetpartial

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"strings"
)

// Conjunto de UUIDs
// TODO: Usar pairing function para encodear esto.
type uuidSet struct {
	dirID string
	uuids []string
}

func newUUIDSet() uuidSet {
	return uuidSet{
		dirID: "",
		uuids: []string{},
	}
}

func (uS *uuidSet) getUuids() []string {
	return uS.uuids
}

func (uS *uuidSet) addUuids(uuids []string) {
	for _, uuid := range uuids {
		uuid_split := strings.Split(uuid, ".")
		if uS.dirID == "" {
			uS.dirID = uuid_split[0]
		}
		if uS.dirID != uuid_split[0] {
			bitacora.Error("ERROR: Distinto Dir ID en paquetes")
		}

		uuid_id := uuid_split[1]
		uS.uuids = append(uS.uuids, []string{uuid_id}...)
	}
}


type packetPartialUuid struct {
	// Lista de todos los UUIDs que componen a este paquete
	uuids uuidSet

	// Si se contiene al paquete EOF, entonces nos guardamos el UUID de dicho
	// paquete.
	// Si no lo tenemos, esto es basura (potenicalmente "").
	// Para obtenerlo, usar getEOFUuid()
	eof_uuid string
}

func (ppu *packetPartialUuid) getEOFUuid() *string {
	if ppu.eof_uuid == "" {
		return &ppu.eof_uuid
	} else {
		return nil
	}
}

func (ppu *packetPartialUuid) getDirID() string {
	dirID := ppu.uuids.dirID
	if dirID == "" {
		panic("ERROR: No DIR Id en packet")
	}

	return dirID
}

// No le pongo partial header porque sino parece incompleto.
type packetPartialHeader struct {
	// ID de la session a la que este paquete corresponde
	session_id string

	packet_uuid packetPartialUuid

	// La IP + puerto del cliente de la session
	client_ip_port string
}


func createPartialHeader(partialPackets []PacketPartial, packets []packet.Packet) packetPartialHeader {
	// Si esto se queda en "", entonces no encontramos el EOF en este batch
	eofUuid := ""
	var uuidsSet uuidSet

	session_id     := ""
	client_ip_port := ""

	for _, partialPacket := range partialPackets {
		uuids := partialPacket.GetUUIDs()
		uuidsSet.addUuids(uuids)

		potentialEOF := partialPacket.getEOFUuid()
		// Si es distinto de Nil, entonces es EOF
		if potentialEOF != nil {
			if eofUuid != "" {
				bitacora.Error("ERROR: Recibi doble EOF en uno de los paquetes normales")
			}
			eofUuid = *potentialEOF
		}

		current_sessionID := partialPacket.GetSessionID()
		// NOTE: Esto es un sanity check
		if session_id == "" {
			session_id = current_sessionID
		}

		if session_id != current_sessionID {
			bitacora.Error(fmt.Sprintf("ERROR: Dos paquetes de distintas sessions se mezclaron. Original: %s, Nuevo: %s", session_id, current_sessionID))
		}

		currentIPort := partialPacket.GetClientAddr()
		if client_ip_port == "" {
			client_ip_port = currentIPort
		}

		if currentIPort != client_ip_port {
			bitacora.Error(fmt.Sprintf("ERROR: Dos paquetes de la misma session tienen dos IPS distintas. Original: %s, Nuevo: %s", client_ip_port, currentIPort))
		}
	}

	for _, packet := range packets {
		uuid := packet.GetUUID()
		uuidsSet.addUuids([]string{uuid})

		if packet.IsEOF() {
			if eofUuid != "" {
				bitacora.Error("ERROR: Recibi doble EOF en uno de los paquetes normales")
			}
			eofUuid = uuid
		}

		currentIPort := packet.GetClientAddr()
		if client_ip_port == "" {
			client_ip_port = currentIPort
		}

		if currentIPort != client_ip_port {
			bitacora.Error(fmt.Sprintf("ERROR: Dos paquetes de la misma session tienen dos IPS distintas. Original: %s, Nuevo: %s", client_ip_port, currentIPort))
		}
	}

	if session_id == "" {
		bitacora.Error("ERROR: Ningun paquete tenia sessionID")
	}

	if client_ip_port == "" {
		bitacora.Error("ERROR: Ningun paquete tenia client_ip_port")
	}

	uuid := packetPartialUuid {
		uuids: uuidsSet,
			eof_uuid: eofUuid,
		}

	header := packetPartialHeader {
		session_id: session_id,
		packet_uuid: uuid,
		client_ip_port: client_ip_port,
		}

	return header
}

// Para crear un paquete parcial, necesito tener una lista de paquetes parciales
// ya creados y/o una lista de paquetes normales.
// Ademas, recibe la funcion de agregacion que se le va a aplicar a todos los
// paquetes.
func CreateParcialPacket(partialPackets []PacketPartial, packets []packet.Packet, aggregator func(payloads []string) string) PacketPartial {
	payloads := make([]string, 0, len(partialPackets) + len(packets))
	for _, partialPacket := range partialPackets {
		payload := partialPacket.GetPayload()
		payloads = append(payloads, payload)
	}
	for _, packet := range packets {
		payload := packet.GetPayload()
		payloads = append(payloads, payload)
	}

	// Aca tengo el payload parcial
	aggregatedPayload := aggregator(payloads)
	header            := createPartialHeader(partialPackets, packets)

	partialPacket     := PacketPartial {
		header: header,
		payload: aggregatedPayload,
	}

	return partialPacket
}


type PacketPartial struct {
	header packetPartialHeader
	payload string
}

// Replica de las funciones de Packet
func (pp *PacketPartial) GetPayload() string {
	return pp.payload
}

// Originalmente en Packet GetUUID
func (pp *PacketPartial) GetUUIDs() []string {
	return pp.header.packet_uuid.uuids.getUuids()
}

func (pp *PacketPartial) GetSessionID() string {
	return pp.header.session_id
}

func (pp *PacketPartial) IsEOF() bool {
	eofUUID := pp.getEOFUuid()
	return eofUUID != nil
}

func (pp *PacketPartial) GetClientAddr() string {
	return pp.header.client_ip_port
}

func (pp *PacketPartial) GetDirID() string {
	return pp.header.packet_uuid.getDirID()
}

func (pp *PacketPartial) getEOFUuid() *string {
	return pp.header.packet_uuid.getEOFUuid()
}


