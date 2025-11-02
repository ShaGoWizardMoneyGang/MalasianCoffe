package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	aggregator "malasian_coffe/system/partial_aggregator/src"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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

	outAmount_s := os.Args[3]
	outAmount, err := strconv.ParseUint(outAmount_s, 10, 64)
	if err != nil {
		panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
	}
	outs := make(map[string]uint64, outAmount)
	for i := range outAmount {
		outputMap := os.Args[4+i]
		splitted := strings.Split(outputMap, ":")
		queueName, queueAmount_s := splitted[0], splitted[1]
		queueAmount, err := strconv.ParseUint(queueAmount_s, 10, 64)
		if err != nil {
			panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
		}

		outs[queueName] = queueAmount
	}

	worker := aggregator.PartialAggregatorBuilder(function, rabbitAddr, outs)
	colaEntrada := worker.GetInput()
	msgQueue := consumeInput(colaEntrada)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		for message := range *msgQueue {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			outboundMessages := worker.Process(pkt)

			for _, outbound := range outboundMessages {
				cola := outbound.ColaSalida
				p := outbound.Packet
				if err := cola.Send(p); err != 0 {
					slog.Error("Error enviando a cola de salida", "err", err)
				}
			}

			if err := message.Ack(false); err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
		}
	}()

	select {
	case <-ctx.Done():
		bitacora.Info("Graceful shutdown solicitado (SIGTERM/SIGINT)")
	}
}
