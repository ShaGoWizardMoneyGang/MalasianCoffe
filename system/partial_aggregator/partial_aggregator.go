package main

import (
	"fmt"
	"malasian_coffe/system/middleware"
	aggregator "malasian_coffe/system/partial_aggregator/src"
	"os"
	"os/signal"
	"runtime/pprof"
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
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        sig := <-c
        println("Received signal:", sig.String())

        // Dump all goroutine stacks to stderr
        pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)

        os.Exit(1)
    }()

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
	worker.Process()
}
