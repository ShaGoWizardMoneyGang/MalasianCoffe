package main

import (
	"bytes"
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	filter_mapper "malasian_coffe/system/filter_mapper/src"
	"malasian_coffe/system/middleware"
	"os"
)

func consumeInput(colaEntrada *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	return msgQueue
}

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
	print("Filter function: ", filterFunction, "\n")
	worker := filter_mapper.FilterMapperBuilder(filterFunction, rabbitAddr)
	colaEntrada := worker.GetInput()

	msgQueue := consumeInput(colaEntrada)

	for message := range *msgQueue { //while true hasta que terminen los mensajes
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		outboundMessages := worker.Process(pkt)

		for _, outbound := range outboundMessages {
			cola := outbound.ColaSalida
			packet := outbound.Packet
			cola.Send(packet.Serialize())
		}

		err := message.Ack(false)
		if err != nil {
			bitacora.Error(fmt.Sprintf("Could not ack, %w", err))
		}
	}
}
