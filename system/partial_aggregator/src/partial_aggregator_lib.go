package partial_aggregator

import (
	"fmt"
	"strings"
)

func PartialAggregatorBuilder(name string, rabbitAddr string, outs map[string]uint64) PartialAggregator {
	switch strings.ToLower(name) {
	case "query3":
		worker := &aggregator3Partial{}
		worker.Build(rabbitAddr, outs)
		return worker
	default:
		panic(fmt.Sprintf("Funcion desconocida '%s'", name))
	}
}
