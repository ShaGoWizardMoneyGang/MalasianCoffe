package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// ========================== Important functions =============================

// Send some data to the network
func SendToNetwork(conn net.Conn, data []byte) error {
	networkPacket := newNetworkPacket(data)
	networkPacket_b := networkPacket.serialize()
	error := send(conn, networkPacket_b)
	return error
}

// Receive from the network
func ReceiveFromNetwork(conn net.Conn) ([]byte, error) {
	indicator, err := read(conn, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to receive network packet indicator: %w", err)
	}
	if indicator[0] != networkpacketindicator {
		return nil, fmt.Errorf("NetworkPacket does not match expected NetworkPacket indicator. Expected %d, found %d", networkpacketindicator, int(indicator[0]))
	}

	size_b, err := read(conn, 8)
	if err != nil {
		return nil, fmt.Errorf("Failed to receive NetworkPacketIndicator size: %w", err)
	}
	size := binary.BigEndian.Uint64(size_b)

	data, err := read(conn, int(size))
	if err != nil {
		return nil, err
	}

	return data, nil
}

/// Helper functions

// Decidido democraticamente por Whatsapp
const networkpacketindicator = 175

type networkPacket struct {
	indicator byte
	size      uint64
	data      []byte
}

func newNetworkPacket(data []byte) networkPacket {
	size := len(data)
	networkPacket := networkPacket{
		indicator: networkpacketindicator,
		size:      uint64(size),
		data:      data,
	}

	return networkPacket
}

func (np *networkPacket) serialize() []byte {
	buffer := make([]byte, len(np.data)+1+8)
	buffer[0] = np.indicator
	binary.BigEndian.PutUint64(buffer[1:9], np.size)
	copy(buffer[9:], np.data)
	return buffer
}

// ============================ Socket wrappers ===============================

// Wrapper function around net.Conn.Write that handles short writes
func send(conn net.Conn, data []byte) error {
	length := len(data)
	if data == nil || length == 0 {
		return fmt.Errorf("tried to send empty data")
	}
	var sent = 0
	var err error
	for offset := 0; offset < length; offset += sent {
		data_send := data[offset:]
		sent, err = conn.Write(data_send)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func read(conn net.Conn, size int) ([]byte, error) {
	buffer := make([]byte, size)

	var received = 0
	var err error
	for offset := 0; offset < size; offset += received {
		received, err = conn.Read(buffer[offset:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("[read func]connection closed while reading: %w", err)
			}
			return nil, err
		}
	}
	return buffer, nil
}

// NOTE: Esto no va a andar en Docker puede ser? Mepa que si.
func AddrToRabbitURI(addr string) string {
	rabbitURI := "amqp://guest:guest@"
	rabbitURI += addr
	return rabbitURI
}
