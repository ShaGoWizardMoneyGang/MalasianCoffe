package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	concat "malasian_coffe/system/concat/src"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"os"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {

	aggregatorFunction := os.Args[2]
	if len(aggregatorFunction) == 0 {
		panic("No filter function provided")
	}
	rabbitAddr := os.Args[1]
	colaEntrada, err := middleware.CreateQueue("COLA DE ENTRADA", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
	}
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}

	colaSalida, err := middleware.CreateQueue("COLA DE SALIDA", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(COLA DE SALIDA): %w", err))
	}

	worker := concat.Concat{}
	var result []packet.Packet
	for message := range *msgQueue {

		slog.Debug("Recibi mensaje")
		packet_reader := bytes.NewReader(message.Body)
		packet, _ := packet.DeserializePackage(packet_reader)

		result = worker.Process(packet)
		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
		if len(result) != 0 {
			slog.Info("Obtuve EOF, mando todo empaquetado a la cola de sending")
			for _, pkt := range result {
				err := colaSalida.Send(pkt.Serialize())
				println(err)
			}
		}
	}
}
