package main

import (
	"bytes"
	"fmt"

	"malasian_coffe/packets/packet"
	counter "malasian_coffe/system/counter/src"
	"malasian_coffe/utils/colas"
	"os"
)

func main() {
	counterFunction := os.Args[2]
	if len(counterFunction) == 0 {
		panic(`No filter function provided, tiene que ser algo del estilo:
make run-filter RUN_FUNCTION=transactions
`)
	}
	rabbitAddr := os.Args[1]

	worker := counter.CounterBuilder(counterFunction, rabbitAddr)

	colaEntrada := worker.GetInput()

	msgQueue := colas.ConsumeInput(colaEntrada)

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
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
