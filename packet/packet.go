package packet

import (
	"errors"
	// "fmt"

	"strconv"

	"malasian_coffe/protocol"
)

// Formato:
// String del estilo A.B.C.D...
// Donde:
// - A: A es el identificador del archivo del cual se obtuvo este paquete
// - El resto de los campos representan la particion del archivo, es decir cuantas particiones y subparticiones tuvo el archivo.
type PacketUuid struct {
	uuid string

	// End of file. Este paquete es el ultimo paquete del archivo correspondiente
	eof bool
}

func (pu *PacketUuid) serialize() ([]byte) {
	uuid_b := protocol.SerializeString(pu.uuid)
	eof_b := protocol.SerializeBool(pu.eof)

	packet_b := append(uuid_b, eof_b...)
	return packet_b
}

type Header struct {
	// ID de la session a la que este paquete corresponde
	session_id uint64

	packet_uuid PacketUuid

	// TODO: esto potencialmente se puede guardar aparte en un nodo que guarde
	// las IPS. No me gusta esa decision porque introducis comunicacion extra.
	// La IP + puerto del cliente de la session
	client_ip_port string
}

func (h *Header) serialize() ([]byte) {
	session_id_b := protocol.SerializeUInteger64(h.session_id)
	packet_b := h.packet_uuid.serialize()
	client_ip_port_b := protocol.SerializeString(h.client_ip_port)

	header_b := append(session_id_b, packet_b...)
	header_b = append(header_b, client_ip_port_b...)

	return header_b
}



func newHeader(session_id uint64, packet_uuid PacketUuid, client_ip_port string) (Header){
	return Header{
		session_id: session_id,
		packet_uuid: packet_uuid,
		client_ip_port: client_ip_port,
	}

}

type Packet struct {
	header Header

	payload []byte
}

func (p *Packet) Serialize() ([]byte) {
	header_b := p.header.serialize()
	packet_b := append(header_b, p.payload...)

	return packet_b
}


// Struct usado para crear varios packets de un mismo directorio
type PacketBuilder struct {
// ============================ Campos de logica ===============================
	// ID del directorio del cual van a ser todos los packets
	dirID uint

	session_id uint64

	currentSequenceNumber uint

	client_ip_port string

// ======================== Campos de sanity checks ===========================

	// Sanity check para corroborar que no envio dos archivos con el indicador de EOF
	already_sent_eof bool
}

func NewPacketBuilder(dirID uint, sessionID uint64, client_ip_port string) (PacketBuilder) {
	return PacketBuilder {
		dirID: dirID,
		currentSequenceNumber: 0,

		session_id: sessionID,

		client_ip_port: client_ip_port,

		already_sent_eof: false,
	}
}

func (pb *PacketBuilder) CreatePacket(payload []byte, is_eof bool) (Packet, error){
	// Sanity checks

	if pb.already_sent_eof && is_eof {
		return Packet{}, errors.New("Tried to send a packet with EOF = true, when a packet with that flag on was alreay sent")
	}

	// Build packet
	packet_id := strconv.FormatUint(uint64(pb.dirID), 10) + "." + strconv.FormatUint(uint64(pb.currentSequenceNumber), 10)
	pb.currentSequenceNumber += 1

	packet_uuid := PacketUuid {
		uuid: packet_id,
		eof: is_eof,
	};

	header := newHeader(pb.session_id, packet_uuid, pb.client_ip_port);

	return Packet {
		header: header,
		payload: payload,
	}, nil

}
