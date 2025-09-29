package packet

import (
	"errors"

	"net"
	"strings"

	"log/slog"
	"strconv"

	"malasian_coffe/utils/network"
)

const (
	// Max batch size es 8192 simplemente porque es el valor default de BUFSIZ en glibc:
	// https://sourceware.org/git/?p=glibc.git;a=blob;f=libio/stdio.h;h=e0e70945fab175fafcb0c8bbae96ad7eebe3df5a;hb=HEAD#l100
	// Ademas, en el tp0 el maximo era 8000, el cual es parecido en tamano
	MAX_BATCH_SIZE int = 8192
)

// Struct usado para crear varios packets de un mismo directorio
type PacketBuilder struct {
	// ============================ Campos de logica ===============================
	// ID del directorio del cual van a ser todos los packets
	dirID uint

	session_id string

	currentSequenceNumber uint

	client_ip_port string

	// =========================== Campos de payload ==============================

	payload_buffer *strings.Builder

	gatewayIP net.Conn

	// ======================== Campos de sanity checks ===========================

	// Sanity check para corroborar que no envio dos archivos con el indicador de EOF
	already_sent_eof bool
}

func NewPacketBuilder(dirID uint, sessionID string, client_ip_port string, gatewayIP net.Conn) PacketBuilder {
	var payload_buffer strings.Builder
	payload_buffer.Grow(MAX_BATCH_SIZE)

	return PacketBuilder{
		dirID:                 dirID,
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

func (pb *PacketBuilder) Send(register string) error {
	if pb.payload_buffer.Len()+len(register) > MAX_BATCH_SIZE {
		packet, err := pb.createPacket(pb.payload_buffer.String(), false)
		if err != nil {
			return err
		}

		err = network.SendToNetwork(pb.gatewayIP, packet.Serialize())
		if err != nil {
			return err
		}

		pb.payload_buffer.Reset()
	}

	pb.payload_buffer.WriteString(register)

	return nil
}

func (pb *PacketBuilder) End() error {
	packet, err := pb.createPacket(pb.payload_buffer.String(), true)

	if err != nil {
		return err
	}

	err = network.SendToNetwork(pb.gatewayIP, packet.Serialize())
	if err != nil {
		return err
	}

	pb.payload_buffer.Reset()

	return nil
}

func (pb *PacketBuilder) createPacket(payload string, is_eof bool) (Packet, error) {
	slog.Info("New packet")
	// Sanity checks

	if pb.already_sent_eof && is_eof {
		return Packet{}, errors.New("Tried to send a packet with EOF = true, when a packet with that flag on was alreay sent")
	}

	// Build packet
	packet_id := strconv.FormatUint(uint64(pb.dirID), 10) + "." + strconv.FormatUint(uint64(pb.currentSequenceNumber), 10)
	pb.currentSequenceNumber += 1

	packet_uuid := PacketUuid{
		uuid: packet_id,
		eof:  is_eof,
	}

	header := newHeader(pb.session_id, packet_uuid, pb.client_ip_port)

	return Packet{
		header:  header,
		payload: payload,
	}, nil

}
