package main

import (
	"bytes"
	"fmt"
	"net"
	"os"

	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/network"
	"malasian_coffe/system/middleware"

)

func main() {
	rabbit_addr := os.Args[2]

	colaTransactions, err := middleware.CreateQueue("DataTransactions", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
	// colaUsers, err := middleware.CreateQueue("DataUsers", middleware.ChannelOptionsDefault())
	// colaStore, err := middleware.CreateQueue("DataStore", middleware.ChannelOptionsDefault())
	// colaTransactionItems, err := middleware.CreateQueue("DataTransactionItems", middleware.ChannelOptionsDefault())
	// colaMenuItems, err := middleware.CreateQueue("DataMenuItems", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf(`failed to rconnect to RabbitMQ: %s. Is the daemon active?
		Try running:

		sudo systemctl start rabbitmq
		or
		sudo rc-service rabbitmq start`, rabbit_addr))
	}

	//listen_addr
	listen_addr := os.Args[1]

	list, err := net.Listen("tcp", listen_addr)
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
			fmt.Errorf("Error deserializing package: %s", err)
		}
		fmt.Printf("%v\n", packet)

		// TODO: esto esta hardcodeado asi porque es para la query 1.
		// Aca deberia haber un switch que lo envie a la queue correspondiente
		if packet.GetDirID() != "3"{
			continue
		}
		colaTransactions.Send(packet.Serialize())
	}
}
