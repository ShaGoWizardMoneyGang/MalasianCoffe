package main

import (
	"fmt"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"

	counter "malasian_coffe/system/counter/src"
	"os"
)

// TODO: MATCHEAR CON "counterN"
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

	counterFunction := os.Args[2]
	if len(counterFunction) == 0 {
		panic(`No filter function provided, tiene que ser algo del estilo:
make run-filter RUN_FUNCTION=transactions
`)
	}
	rabbitAddr := os.Args[1]

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

	worker := counter.CounterBuilder(counterFunction, rabbitAddr, outs)
	worker.Process()
}
