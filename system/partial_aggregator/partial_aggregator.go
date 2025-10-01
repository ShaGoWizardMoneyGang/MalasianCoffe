package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	aggregator "malasian_coffe/system/partial_aggregator/src"
	"os"
)

func consumeInput(colaEntrada *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	return msgQueue
}

func main() {

	rabbitAddr := os.Args[1]
	function := os.Args[2]
	if len(function) == 0 {
		panic("No aggregator function provided")
	}

	worker := aggregator.PartialAggregatorBuilder(function, rabbitAddr)
	colaEntrada := worker.GetInput()
	msgQueue := consumeInput(colaEntrada)

	for message := range *msgQueue {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		outboundMessages := worker.Process(pkt)

		for _, outbound := range outboundMessages {
			cola := outbound.ColaSalida
			p := outbound.Packet
			if err := cola.Send(p.Serialize()); err != 0 {
				slog.Error("Error enviando a cola de salida", "err", err)
			}
		}

		if err := message.Ack(false); err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
