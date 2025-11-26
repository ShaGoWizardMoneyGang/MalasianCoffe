package counter

import (
	"fmt"
	"strings"
)

type Counter interface {
	Build(rabbitAddr string, queueAmounts map[string]uint64)
	Process()
}

func CounterBuilder(counterName string, rabbitAddr string, queueAmounts map[string]uint64) Counter {
	var counter Counter
	switch strings.ToLower(counterName) {
	case "query4":
		counter = &counterQuery4{}
	case "query2a":
		counter = &counterQuery2a{}
	case "query2b":
		counter = &counterQuery2b{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr, queueAmounts)

	return counter
}
