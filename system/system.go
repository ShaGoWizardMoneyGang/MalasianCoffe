package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/dataset"
	"malasian_coffe/utils/network"
)

func main() {
	rabbit_addr := os.Args[2]

	colaTransactions, err := middleware.CreateQueue("DataTransactions", middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaUsers, err := middleware.CreateQueue("DataUsers", middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaStore, err := middleware.CreateQueue("DataStore", middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaTransactionItems, err := middleware.CreateQueue("DataTransactionItems", middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaMenuItems, err := middleware.CreateQueue("DataMenuItems", middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
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
		packet_id, err := strconv.ParseUint(packet.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}
		switch dataset_name {
		case "menu_items":
			colaMenuItems.Send(packet.Serialize())
		case "stores":
			colaStore.Send(packet.Serialize())
		case "transaction_items":
			colaTransactionItems.Send(packet.Serialize())
		case "transactions":
			colaTransactions.Send(packet.Serialize())
		case "users":
			colaUsers.Send(packet.Serialize())
		}
	}
}
