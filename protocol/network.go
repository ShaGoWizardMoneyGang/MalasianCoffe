// Este archivo se encarga de enviar y recibir datos a la red
package protocol

import (
	"encoding/binary"
	"fmt"
	"net"

	"malasian_coffe/utils/network"
)

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

func SendToNetwork(conn net.Conn, data []byte) {
	networkPacket := newNetworkPacket(data)
	networkPacket_b := networkPacket.serialize()
	network.Send(conn, networkPacket_b)
}

func ReceiveFromNetwork(conn net.Conn) ([]byte, error) {
	indicator, err := network.Read(conn, 1);
	if err != nil {
		return nil, fmt.Errorf("Failed to receive NetworkPacketIndicator")

	}
	if indicator[0] != networkpacketindicator {
		return nil, fmt.Errorf("NetworkPacket does not match expected NetworkPacket indicator. Expected %d, found %d", networkpacketindicator, int(indicator[0]))
	}

	size_b, err := network.Read(conn, 8);
	if err != nil {
		return nil, fmt.Errorf("Failed to receive NetworkPacketIndicator size")
	}
	size := binary.BigEndian.Uint64(size_b)

	data, err := network.Read(conn, int(size))

	return data, nil
}
