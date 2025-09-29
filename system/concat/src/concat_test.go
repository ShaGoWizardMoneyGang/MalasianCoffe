package concat

import (
	"malasian_coffe/packets/packet"
	"testing"
)

func TestConcatFunctionQuery1(t *testing.T) {
	c := &Concat{}

	// primer paquete
	paquete1 := packet.Packet{}
	paquete1 = packet.ChangePayload(paquete1, []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n"})[0]
	salida1 := c.Process(paquete1)[0]
	payload1 := salida1.GetPayload()
	esperado1 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n"
	if payload1 != esperado1 {
		panic("Fallo en salida 1")
	}

	// segundo paquete
	paquete2 := packet.Packet{}
	paquete2 = packet.ChangePayload(paquete2, []string{"7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n"})[0]
	salida2 := c.Process(paquete2)[0]
	payload2 := salida2.GetPayload()
	esperado2 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n"
	if payload2 != esperado2 {
		panic("Fallo en salida 2")
	}

	// tercer paquete (sin \n al final, concat lo agrega)
	paquete3 := packet.Packet{}
	paquete3 = packet.ChangePayload(paquete3, []string{"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8"})[0]
	salida3 := c.Process(paquete3)[0]
	payload3 := salida3.GetPayload()
	esperado3 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,20\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,15\n" +
		"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8\n"
	if payload3 != esperado3 {
		panic("Fallo en salida 3")
	}
}
