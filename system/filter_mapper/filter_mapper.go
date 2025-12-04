package main

import (
	"fmt"
	filter_mapper "malasian_coffe/system/filter_mapper/src"
	"malasian_coffe/system/middleware"
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

// Cola input menu items: DataMenuItems TODO: constante global (utils)
// Cola output menu items filtrados: FilteredMenuItems
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



	filterFunction := os.Args[2]
	if len(filterFunction) == 0 {
		panic(`No filter function provided, tiene que ser algo del estilo:
make run-filter RUN_FUNCTION=transactions
`)
	}
	rabbitAddr := os.Args[1]
	print("Filter function: ", filterFunction, "\n")

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


	worker := filter_mapper.FilterMapperBuilder(filterFunction, rabbitAddr, outs)

	worker.Process()
}
