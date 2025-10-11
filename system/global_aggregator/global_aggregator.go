package main

import (
	"fmt"
	"malasian_coffe/bitacora"
	aggregator "malasian_coffe/system/global_aggregator/src"
	"os"
)

// Argumentos:
// 1) Address de Rabbit
// 2) Nombre del aggregator global (query2a, query2b, query3, query4)
func main() {
	rabbitAddr := os.Args[1]
	aggName := os.Args[2]

	worker := aggregator.GlobalAggregatorBuilder(aggName, rabbitAddr)
	bitacora.Info(fmt.Sprintf("Starting global aggregator %s", aggName))

	worker.Process()
}
