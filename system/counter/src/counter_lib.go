package counter

import (
	"fmt"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
)

type Counter interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string)

	// Devuelve referencia de la cola de la cual tiene que consumir
	GetInput() *middleware.MessageMiddlewareQueue

	// Funcio que hace el filtrado
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

func CounterBuilder(counterName string, rabbitAddr string) Counter {
	var counter Counter
	switch strings.ToLower(counterName) {
	case "query4":
		counter = &CounterQuery4{}
	case "query2a":
		counter = &CounterQuery2a{}
	case "query2b":
		counter = &CounterQuery2b{}
	default:
		panic(fmt.Sprintf("Unknown counter %s", counterName))
	}

	counter.Build(rabbitAddr)

	return counter
}
