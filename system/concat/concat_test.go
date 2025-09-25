package concat

import (
	"malasian_coffe/packet"
	"testing"
)

func TestConcatFunctionQuery1(t *testing.T) {
	c := &Concat{Function: concatFunctionQuery}

	// primer paquete
	paquete1 := packet.Packet{Payload: []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n")}
	salida1 := c.Process(paquete1)
	esperado1 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n"
	if string(salida1.Payload) != esperado1 {
		panic("Fallo en salida 1")
	}

	// segundo paquete
	paquete2 := packet.Packet{Payload: []byte("7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n")}
	salida2 := c.Process(paquete2)
	esperado2 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n"
	if string(salida2.Payload) != esperado2 {
		panic("Fallo en salida 2")
	}

	// tercer paquete
	paquete3 := packet.Packet{Payload: []byte("51e44c8e-4812-4a15-a9f9-9a46b62424d6,8")}
	salida3 := c.Process(paquete3)
	esperado3 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n51e44c8e-4812-4a15-a9f9-9a46b62424d6,8\n"
	if string(salida3.Payload) != esperado3 {
		panic("Fallo en salida 3")
	}
}
