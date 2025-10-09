package packet

import (
	"errors"
	"fmt"

	"net"
	"strings"

	"strconv"

	"malasian_coffe/bitacora"
	"malasian_coffe/utils/dataset"
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
	directory_name string

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

func NewPacketBuilder(directory_name string, sessionID string, client_ip_port string, gatewayIP net.Conn) PacketBuilder {
	var payload_buffer strings.Builder
	payload_buffer.Grow(MAX_BATCH_SIZE)

	return PacketBuilder{
		directory_name: directory_name,
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
	defer pb.gatewayIP.Close()

	packet, err := pb.createPacket(pb.payload_buffer.String(), true)

	if err != nil {
		return err
	}

	err = network.SendToNetwork(pb.gatewayIP, packet.Serialize())
	if err != nil {
		return err
	}

	pb.payload_buffer.Reset()

	bitacora.Info(fmt.Sprintf("Envie %d paquetes, del dataset %s", pb.currentSequenceNumber, pb.directory_name))


	return nil
}


func (pb *PacketBuilder) createPacket(payload string, is_eof bool) (Packet, error) {
	// Sanity checks

	if pb.already_sent_eof && is_eof {
		return Packet{}, errors.New("Tried to send a packet with EOF = true, when a packet with that flag on was alreay sent")
	}

	dir_id, err := dataset.DatasetToID(pb.directory_name)
	if err != nil {
		return Packet{}, err
	}
	// Build packet
	packet_id := strconv.FormatUint(dir_id, 10) + "." + strconv.FormatUint(uint64(pb.currentSequenceNumber), 10)
	pb.currentSequenceNumber += 1

	packet_uuid := packetUuid{
		uuid: packet_id,
		eof:  is_eof,
	}

	header := newHeader(pb.session_id, packet_uuid, pb.client_ip_port)

	return Packet{
		header:  header,
		payload: payload,
	}, nil

}
