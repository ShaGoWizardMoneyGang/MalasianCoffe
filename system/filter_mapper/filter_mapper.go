package main

import (
	"bytes"
	"fmt"
	// "log/slog"
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

// func sendPackets(msgQueue middleware.ConsumeChannel, colaSalida *middleware.MessageMiddlewareQueue, filterFunction string) {
// 	worker := filter_mapper.FilterMapper{}
// 	for message := range *msgQueue {
// 		packetReader := bytes.NewReader(message.Body)
// 		pkt, _ := packet.DeserializePackage(packetReader)

// 		paquetesSalida := worker.Process(pkt, filterFunction)

// 		for _, pkt := range paquetesSalida {
// 			slog.Info("Mando packet filtrado a siguiente cola")
// 			_ = colaSalida.Send(pkt.Serialize())
// 		}
// 		err := message.Ack(false)
// 		if err != nil {
// 			panic(fmt.Errorf("Could not ack, %w", err))
// 		}
// 	}
// }

// Cola input menu items: DataMenuItems TODO: constante global (utils)
// Cola output menu items filtrados: FilteredMenuItems


func main() {
	filterFunction := os.Args[2]
	if len(filterFunction) == 0 {
		panic(`No filter function provided, tiene que ser algo del estilo:
make run-filter RUN_FUNCTION=transactions
`)
	}
	rabbitAddr := os.Args[1]

	worker := filter_mapper.FilterMapperBuilder(filterFunction, rabbitAddr)
	colaEntrada := worker.GetInput()

	msgQueue := consumeInput(colaEntrada)

	for message := range *msgQueue { //while true hasta que terminen los mensajes
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		outboundMessages := worker.Process(pkt)
		// fmt.Printf("PAQUETES SALIDA %v\n", paquetesSalida)

		println("Mandando paquetes a las colas correspondientes")
		// fmt.Printf("PAQUETES PARA COLA 1: %v\n", paquetesSalida[0])

		for _, outbound := range outboundMessages {
			cola := outbound.ColaSalida
			packet := outbound.Packet
			cola.Send(packet.Serialize())
		}

		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
