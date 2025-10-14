package global_aggregator

import (
	"fmt"
	"strings"
	// "malasian_coffe/system/queries/query4"
)

type GlobalAggregator interface {
	Build(rabbitAddr string, routing_key string, outs map[string]uint64)
	Process()
}

func GlobalAggregatorBuilder(name, rabbitAddr string, routing_key string, outs map[string]uint64) GlobalAggregator {
	var globalAggregator GlobalAggregator
	switch strings.ToLower(name) {
	case "query2a":
		globalAggregator = &aggregator2aGlobal{}
	case "query2b":
		globalAggregator = &aggregator2bGlobal{}
	case "query3":
		globalAggregator = &aggregator3Global{}
	case "query4":
		globalAggregator = &aggregator4Global{}
	default:
		panic(fmt.Sprintf("Unknown global aggregator '%s'", name))
	}
	globalAggregator.Build(rabbitAddr, routing_key, outs)
	return globalAggregator
}
