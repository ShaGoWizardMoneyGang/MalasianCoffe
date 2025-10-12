package counter

import (
	"fmt"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
)

type Counter interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string, queueAmounts map[string] uint64)

	// Devuelve referencia de la cola de la cual tiene que consumir
	GetInput() *middleware.MessageMiddlewareQueue

	// Funcio que hace el filtrado
	Process(pkt packet.Packet) []colas.OutBoundMessage
}

func CounterBuilder(counterName string, rabbitAddr string, queueAmounts map[string] uint64) Counter {
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
