package packet

import (
	"bytes"

	"malasian_coffe/protocol"
)

// =============================== PacketUUID ==================================

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

// ================================= Header ====================================

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

// ================================= Packet ====================================

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
