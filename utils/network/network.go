package network

import (
	"encoding/binary"
	"fmt"
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
	indicator, err := read(conn, 1);
	if err != nil {
		return nil, fmt.Errorf("Failed to receive NetworkPacketIndicator")

	}
	if indicator[0] != networkpacketindicator {
		return nil, fmt.Errorf("NetworkPacket does not match expected NetworkPacket indicator. Expected %d, found %d", networkpacketindicator, int(indicator[0]))
	}

	size_b, err := read(conn, 8);
	if err != nil {
		return nil, fmt.Errorf("Failed to receive NetworkPacketIndicator size")
	}
	size := binary.BigEndian.Uint64(size_b)

	data, err := read(conn, int(size))

	return data, nil
}

/// Helper functions

// Decidido democraticamente por Whatsapp
const networkpacketindicator = 175

type networkPacket struct {
	indicator byte
	size uint64
	data []byte
}

func newNetworkPacket(data []byte) networkPacket {
	size := len(data)
	networkPacket := networkPacket {
		indicator: networkpacketindicator,
		size: uint64(size),
		data: data,
	}

	return networkPacket
}

func (np *networkPacket) serialize() []byte {
	buffer := make([]byte, len(np.data) + 1 + 8)
	buffer[0] = np.indicator
	binary.BigEndian.PutUint64(buffer[1:9], np.size)
	copy(buffer[9:], np.data)
	return buffer
}




// ============================ Socket wrappers ===============================

// Wrapper function around net.Conn.Write that handles short writes
func send(conn net.Conn, data []byte) error {
	length := len(data)

	var sent = 0
	var err error
	for offset := 0 ; offset < length ; offset += sent {
		sent, err = conn.Write(data[offset:])
		if err != nil {
			return err
		}
	}

	return nil
}

func read(conn net.Conn, size int) ([]byte, error) {
	buffer := make([]byte, size)

	var received = 0
	var err error
	for offset := 0 ; offset < size ; offset += received {
		received, err = conn.Read(buffer[received:])
		if err != nil {
			break
		}
	}

	return buffer, err
}
