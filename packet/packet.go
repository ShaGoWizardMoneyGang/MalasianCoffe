package packet

import (
	"bytes"
	"errors"

	"net"
	"strings"

	"strconv"

	"malasian_coffe/protocol"

	"log/slog"
)

const (
	// Max batch size es 8192 simplemente porque es el valor default de BUFSIZ en glibc:
	// https://sourceware.org/git/?p=glibc.git;a=blob;f=libio/stdio.h;h=e0e70945fab175fafcb0c8bbae96ad7eebe3df5a;hb=HEAD#l100
	// Ademas, en el tp0 el maximo era 8000, el cual es parecido en tamano
	MAX_BATCH_SIZE int  = 8192
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

func deserializePacketUuid(reader *bytes.Reader) (PacketUuid, error) {
	uuid, error := protocol.DeserializeString(reader)
	if error != nil {
		return PacketUuid{}, error
	}
	eof, error := protocol.DeserializeBool(reader)
	if error != nil {
		return PacketUuid{}, error
	}

	return PacketUuid{
		uuid: uuid,
		eof: eof,
	}, nil
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

func deserializeHeader(reader *bytes.Reader) (Header, error){
	session_id, error  := protocol.DeserializeUInteger64(reader)
	if error != nil {
		return Header{}, error
     }
	packet_uuid, error := deserializePacketUuid(reader)
	if error != nil {
		return Header{}, error
     }

	client_ip_port, error := protocol.DeserializeString(reader)
	if error != nil {
		return Header{}, error
     }

	header := Header {
		session_id: session_id,
		packet_uuid: packet_uuid,
		client_ip_port: client_ip_port,
	}

	return header, nil
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

	payload string
}

func (p *Packet) Serialize() ([]byte) {
	header_b := p.header.serialize()
	payload  := protocol.SerializeString(p.payload)
	packet_b := append(header_b, payload...)

	return packet_b
}

func DeserializePackage(reader *bytes.Reader) (Packet, error) {
	header, error := deserializeHeader(reader)
	if error != nil {
		return Packet{}, error
     }
	// NOTE: En teoria, la data deberia estar consumida a este punto
	payload, error := protocol.DeserializeString(reader)
	if error != nil {
		return Packet{}, error
     }

	packet := Packet {
		header: header,
		payload: payload,
	}

	return packet, nil
}


// Struct usado para crear varios packets de un mismo directorio
type PacketBuilder struct {
// ============================ Campos de logica ===============================
	// ID del directorio del cual van a ser todos los packets
	dirID uint

	session_id uint64

	currentSequenceNumber uint

	client_ip_port string

// =========================== Campos de payload ==============================

	payload_buffer *strings.Builder

	gatewayIP *net.Conn

// ======================== Campos de sanity checks ===========================

	// Sanity check para corroborar que no envio dos archivos con el indicador de EOF
	already_sent_eof bool
}

func NewPacketBuilder(dirID uint, sessionID uint64, client_ip_port string, gatewayIP *net.Conn) (PacketBuilder) {
	var payload_buffer strings.Builder
	payload_buffer.Grow(MAX_BATCH_SIZE)

	return PacketBuilder {
		dirID: dirID,
		currentSequenceNumber: 0,

		session_id: sessionID,

		client_ip_port: client_ip_port,

		gatewayIP: gatewayIP,

		// Payload
		payload_buffer: &payload_buffer,

		// Sanity
		already_sent_eof: false,
	}
}

func sendToSocket(conn *net.Conn, data []byte) error {
	length := len(data)

	var sent = 0
	var err error
	for offset := 0 ; offset < length ; offset += sent {
		sent, err = (*conn).Write(data[offset:])
		if err != nil {
			return err
		}
	}

	return nil
}

func (pb *PacketBuilder) Send(register string) (error) {
	if pb.payload_buffer.Len() + len(register) > MAX_BATCH_SIZE {
		packet, err := pb.createPacket(pb.payload_buffer.String(), false)
		if err != nil {
			return err
		}

		err = sendToSocket(pb.gatewayIP, packet.Serialize())
		if err != nil {
			return err
		}

		pb.payload_buffer.Reset()
	}

	pb.payload_buffer.WriteString(register)

	return nil
}

func (pb *PacketBuilder) End() (error) {
	packet, err := pb.createPacket(pb.payload_buffer.String(), true)

	if err != nil {
		return err
	}

	err = sendToSocket(pb.gatewayIP, packet.Serialize())
	if err != nil {
		return err
	}

	pb.payload_buffer.Reset()

	return nil
}

func (pb *PacketBuilder) createPacket(payload string, is_eof bool) (Packet, error){
	slog.Info("New packet")
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

