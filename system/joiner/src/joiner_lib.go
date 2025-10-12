package joiner

import (
	"fmt"
)

type Joiner interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string, routingKey string)

	// Funcio que hace el filtrado
	Process()
}

func JoinerBuilder(joinerName string, rabbitAddr string, routingKey string) Joiner {
	var joiner Joiner

	switch joinerName {
	case "Query2a":
		joiner = &joinerQuery2a{}
	case "Query2b":
		joiner = &joinerQuery2b{}
	case "Query3":
		joiner = &joinerQuery3{}
	case "Query4":
		joiner = &joinerQuery4{}
	default:
		panic(fmt.Sprintf("Unknown 'joiner' %s", joinerName))
	}

	joiner.Build(rabbitAddr, routingKey)

	return joiner
}
