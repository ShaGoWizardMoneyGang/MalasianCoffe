package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	filter_mapper "malasian_coffe/system/filter_mapper/src"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"os"
)

func instanceQueue(inputQueueName string, rabbitAddr string) *middleware.MessageMiddlewareQueue {
	cola, err := middleware.CreateQueue(inputQueueName, middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", inputQueueName, err))
	}
	return cola
}

func consumeInput(colaEntrada *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	return msgQueue
}

func sendPackets(msgQueue middleware.ConsumeChannel, colaSalida *middleware.MessageMiddlewareQueue, filterFunction string) {
	worker := filter_mapper.FilterMapper{}
	var result []packet.Packet
	for message := range *msgQueue {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		paqueteSalida := worker.Process(pkt, filterFunction)
		result = []packet.Packet{paqueteSalida}

		for _, pkt := range result {
			slog.Info("Mando packet filtrado a siguiente cola")
			_ = colaSalida.Send(pkt.Serialize())
		}
		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}

// Cola input menu items: DataMenuItems
// Cola output menu items filtrados: FilteredMenuItems

func main() {
	filterFunction := os.Args[2]
	if len(filterFunction) == 0 {
		panic("No filter function provided")
	}
	rabbitAddr := os.Args[1]
	// Hay que ponerle el arg del rabbit

	switch filterFunction {
	case "menuItems":
		colaEntrada := instanceQueue("DataMenuItems", rabbitAddr)
		msgQueue := consumeInput(colaEntrada)
		colaSalida := instanceQueue("FilteredMenuItems", rabbitAddr)
		sendPackets(msgQueue, colaSalida, "query2MenuItems")
	case "transactions":
		colaEntrada := instanceQueue("DataTransactions", rabbitAddr)
		msgQueue := consumeInput(colaEntrada)
		colaSalida := instanceQueue("FilteredTransactions", rabbitAddr)
		sendPackets(msgQueue, colaSalida, "query1Transactions")
	}
}
