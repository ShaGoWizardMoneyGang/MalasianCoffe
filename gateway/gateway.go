package main

import (
	"fmt"
	// "net"

	"os"

	"net"

	// "bufio"
	"log/slog"

	// "malasian_coffe/packet"
	// "malasian_coffe/protocol"


	"malasian_coffe/utils/uuid"
	"malasian_coffe/protocol"
)

func main() {
	// Directory with all dataset subdirectories
	gateway_addr := os.Args[1]

	list, err := net.Listen("tcp", gateway_addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
	}
	defer list.Close()

	for {

		// Wait for a connection.

		conn, err := list.Accept()

		if err != nil {

			slog.Error("%s", err)

		}

		// Handle the connection in a new goroutine.

		// The loop then returns to accepting, so that

		// multiple connections may be served concurrently.

		go handle_connection(conn)
	}

}

func handle_connection(conn net.Conn) {
	session_id   := uuid.GenerateUUID()
	session_id_b := protocol.SerializeString(session_id)

	protocol.SendToNetwork(conn, session_id_b)
}
