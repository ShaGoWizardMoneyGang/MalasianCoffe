package main

import (
	joiner "malasian_coffe/system/joiner/src"
	"os"
)

func main() {
	joinFunction := os.Args[2]
	if len(joinFunction) == 0 {
		panic("No join function provided")
	}
	rabbitAddr := os.Args[1]

	joiner := joiner.JoinerBuilder(joinFunction, rabbitAddr)

	joiner.Process()
}
