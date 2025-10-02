package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	aggregator "malasian_coffe/system/global_aggregator/src"
	"malasian_coffe/system/middleware"
	"os"
)

func consumeInput(q *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgs, code := q.StartConsuming()
	if code != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", code))
	}
	return msgs
}

// Argumentos:
// 1) Address de Rabbit
// 2) Nombre del aggregator global (query3Global)
func main() {
	rabbitAddr := os.Args[1]
	aggName := os.Args[2]

	worker := aggregator.GlobalAggregatorBuilder(aggName, rabbitAddr)
	colaEntrada := worker.GetInput()
	msgQueue := consumeInput(colaEntrada)

	for message := range *msgQueue {
		reader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(reader)

		outMsgs := worker.Process(pkt)

		for _, out := range outMsgs {
			slog.Info("Sending packet to joiner")
			fmt.Printf("%v", pkt)
			_ = out.ColaSalida.Send(out.Packet.Serialize())
		}

		if err := message.Ack(false); err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
