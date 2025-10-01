package main

import (
	"bytes"
	"fmt"
	"net"
	"os"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
)

func main() {
	rabbit_addr := os.Args[2]
	//QUERY 2 USA TRANSACTIONS Y STORES
	colaTransactions, err := middleware.CreateQueue("DataTransactions", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
	colaUsers, err := middleware.CreateQueue("DataUsers", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
	colaStore, err := middleware.CreateQueue("DataStore", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
	colaTransactionItems, err := middleware.CreateQueue("DataTransactionItems", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
	colaMenuItems, err := middleware.CreateQueue("DataMenuItems", middleware.ChannelOptions{network.AddrToRabbitURI(rabbit_addr)})
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

		switch packet.GetDirID() {
		case "0":
			colaMenuItems.Send(packet.Serialize())
		case "1":
			colaStore.Send(packet.Serialize())
		case "2":
			colaTransactionItems.Send(packet.Serialize())
		case "3":
			colaTransactions.Send(packet.Serialize())
		case "4":
			colaUsers.Send(packet.Serialize())
		default:
			panic("ID de paquete desconocido")
		}

	}
}
