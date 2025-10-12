package main

import (
	joiner "malasian_coffe/system/joiner/src"
	"os"
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

	joiner.Process()
}
