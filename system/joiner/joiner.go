package main

import (
	joiner "malasian_coffe/system/joiner/src"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

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

	joinFunction := os.Args[2]
	if len(joinFunction) == 0 {
		panic(`No joiner function provided, tiene que ser algo del estilo:
make run-joiner RUN_FUNCTION=Query3
`)
	}
	rabbitAddr := os.Args[1]

	routingKey_s := os.Args[3]

	joiner := joiner.JoinerBuilder(joinFunction, rabbitAddr, routingKey_s)

	joiner.Process()
}
