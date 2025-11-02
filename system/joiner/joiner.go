package main

import (
	"context"
	"malasian_coffe/bitacora"
	joiner "malasian_coffe/system/joiner/src"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	joinFunction := os.Args[2]
	if len(joinFunction) == 0 {
		panic(`No joiner function provided, tiene que ser algo del estilo:
make run-joiner RUN_FUNCTION=Query3
`)
	}
	rabbitAddr := os.Args[1]

	routingKey_s := os.Args[3]

	joiner := joiner.JoinerBuilder(joinFunction, rabbitAddr, routingKey_s)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		joiner.Process()
	}()

	select {
	case <-ctx.Done():
		bitacora.Info("Graceful shutdown solicitado (SIGTERM/SIGINT)")
	}

}
