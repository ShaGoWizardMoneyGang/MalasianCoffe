package main

import (
	"fmt"
	"bytes"
	"net"
	"os"

	"malasian_coffe/packet"

	"malasian_coffe/utils/network"
)

func main() {
	rabbit_addr := os.Args[1]

	list, err := net.Listen("tcp", rabbit_addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
	}
	conn, _ := list.Accept()

	for {
		packet_b, err := network.ReceiveFromNetwork(conn)
		packet_reader := bytes.NewReader(packet_b)
		if err != nil {
			panic(err)
		}
		packet, err := packet.DeserializePackage(packet_reader)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", packet)
	}
}
