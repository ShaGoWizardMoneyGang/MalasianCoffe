package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	counter "malasian_coffe/system/counter/src"
	"malasian_coffe/utils/colas"
	"os"
)

// TODO: MATCHEAR CON "counterN"
func main() {
	counterFunction := os.Args[2]
	if len(counterFunction) == 0 {
		panic(`No filter function provided, tiene que ser algo del estilo:
make run-filter RUN_FUNCTION=transactions
`)
	}
	rabbitAddr := os.Args[1]


	outAmount_s := os.Args[3]
	outAmount, err   := strconv.ParseUint(outAmount_s, 10, 64)
	if err != nil {
		panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
	}
	outs := make(map[string]uint64, outAmount)
	for i := range outAmount {
		outputMap := os.Args[4 + i]
		splitted  := strings.Split(outputMap, ":")
		queueName, queueAmount_s := splitted[0], splitted[1]
		queueAmount, err := strconv.ParseUint(queueAmount_s, 10, 64)
		if err != nil {
			 panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
		}

		outs[queueName] = queueAmount
	}

	worker := counter.CounterBuilder(counterFunction, rabbitAddr, outs)
	slog.Info("Counter builded")

	colaEntrada := worker.GetInput()

	msgQueue := colas.ConsumeInput(colaEntrada)
	slog.Info("Leo de cola entrada", "queue", colaEntrada)
	for message := range *msgQueue { //while true hasta que terminen los mensajes
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		outboundMessages := worker.Process(pkt)
		for _, outbound := range outboundMessages {
			cola := outbound.ColaSalida
			packet := outbound.Packet
			cola.Send(packet)
		}

		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
