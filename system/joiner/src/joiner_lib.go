package joiner

import "fmt"

type Joiner interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string)

	// Funcio que hace el filtrado
	Process()
}


func JoinerBuilder(joinerName string, rabbitAddr string) Joiner {
	var joiner Joiner;

	switch joinerName {
	case "Query3":
		joiner = &joinerQuery3{}
	default:
		panic(fmt.Sprintf("Unknown 'joiner' %s", joinerName))
	}

	joiner.Build(rabbitAddr)

	return joiner
}
