package main

import (
	"context"
	"fmt"
	"malasian_coffe/bitacora"
	aggregator "malasian_coffe/system/global_aggregator/src"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

// Argumentos:
// 1) Address de Rabbit
// 2) Nombre del aggregator global (query2a, query2b, query3, query4)
func main() {
	rabbitAddr := os.Args[1]
	aggName := os.Args[2]

	routing_key := os.Args[3]

	outAmount_s := os.Args[4]
	outAmount, err := strconv.ParseUint(outAmount_s, 10, 64)
	if err != nil {
		panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
	}

	outs := make(map[string]uint64, outAmount)
	for i := range outAmount {
		outputMap := os.Args[5+i]
		splitted := strings.Split(outputMap, ":")
		queueName, queueAmount_s := splitted[0], splitted[1]
		queueAmount, err := strconv.ParseUint(queueAmount_s, 10, 64)
		if err != nil {
			panic(fmt.Errorf("Failed to parse amount of outs %s, %w", outAmount_s, err))
		}

		outs[queueName] = queueAmount
	}

	worker := aggregator.GlobalAggregatorBuilder(aggName, rabbitAddr, routing_key, outs)
	bitacora.Info(fmt.Sprintf("Starting global aggregator %s", aggName))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	done := make(chan struct{})
	go func() {
		worker.Process()
		close(done)
	}()

	select {
	case <-ctx.Done():
		bitacora.Info("Graceful shutdown solicitado (SIGTERM/SIGINT)")
	case <-done:
		bitacora.Info("Procesamiento finalizado normalmente")
	}
}
