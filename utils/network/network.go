package network

import (
	"net"
)

// Wrapper function around net.Conn.Write that handles short writes
func Send(conn net.Conn, data []byte) error {
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

func Read(conn net.Conn, size int) ([]byte, error) {
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
