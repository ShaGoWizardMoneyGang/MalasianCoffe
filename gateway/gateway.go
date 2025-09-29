package main

import (
	"fmt"
	"io"
	// "net"

	"os"

	"net"

	"malasian_coffe/protocol"
	"malasian_coffe/utils/network"
	"malasian_coffe/utils/uuid"
)

func main() {
	// Own gateways addr
	gateway_addr := os.Args[1]

	// Rabbit server addr
	rabbit_addr := os.Args[2]
	rabbit_conn, err := net.Dial("tcp", rabbit_addr)

	list, err := net.Listen("tcp", gateway_addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
	}
	defer list.Close()

	for {

		// Wait for a connection.

		conn, err := list.Accept()

		if err != nil {

			panic(err)

		}

		// Handle the connection in a new goroutine.

		// The loop then returns to accepting, so that

		// multiple connections may be served concurrently.

		go handle_connection(conn, rabbit_conn)
	}

}

func handle_connection(conn net.Conn, system net.Conn) {
	defer conn.Close()
	session_id := uuid.GenerateUUID()
	session_id_b := protocol.SerializeString(session_id)

	network.SendToNetwork(conn, session_id_b)

	for {
		packet, err := network.ReceiveFromNetwork(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Unkown error : %s\n", err)
			}
			break
		}
		network.SendToNetwork(system, packet)
	}
}
