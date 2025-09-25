package concat

import (
	"malasian_coffe/packet"
)

type concatOptions func(*Concat, string) string

var (
	// Recibe paquetes con payload transaction_id, final_amouunt
	concatFunctionQuery concatOptions = func(c *Concat, input string) string {
		if input[len(input)-1] != '\n' {
			input = input + "\n" //agrego el barra n por si no lo tiene
		}
		c.result += input
		return c.result
	}
)

type Concat struct {
	Function concatOptions
	// Guardo el resultado, recuerdo que voy a tener un nodo concat por query!!!
	// el unico caso especial es la de la consulta 4 que devuelve 2 tablas
	result string
}

func (c *Concat) Process(pkt packet.Packet) packet.Packet {
	returned := packet.Packet{Payload: []byte("")}
	if c.Function != nil {
		res := c.Function(c, string(pkt.Payload))
		returned.Payload = []byte(res)
	}
	return returned
}
