package concat

import (
	"malasian_coffe/packet"
)

type Concat struct {
	// Guardo el resultado, recuerdo que voy a tener un nodo concat por query!!!
	// el unico caso especial es la de la consulta 4 que devuelve 2 tablas
	result string
}

func (c *Concat) concatFunctionQuery(input string) string {
	if len(input) > 0 && input[len(input)-1] != '\n' { //puedo tener un input vacio por si creo un packet nuevo
		input += "\n"
	}
	c.result += input
	return c.result
}

func (c *Concat) Process(pkt packet.Packet) []packet.Packet {
	input := pkt.GetPayload()
	output := c.concatFunctionQuery(input)
	outputs := []string{output}
	newPacket := packet.ChangePayload(pkt, outputs)

	return newPacket
}
