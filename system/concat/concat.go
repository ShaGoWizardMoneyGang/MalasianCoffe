package main

import (
	"bytes"
	"fmt"
	"malasian_coffe/packet"
	concat "malasian_coffe/system/concat/src"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {
	type Colas struct {
		colaInput  []string
		colaOutput []string
	}
	funcionesYColas := make(map[string]Colas)
	funcionesYColas["query1234"] = Colas{colaInput: []string{"FilterMapper1YearAndAmount"}, colaOutput: []string{"Concat1"}}
	// funcionesYColas["query2"] = Colas{colaInput: []string{"entrada-2"}, colaOutput: []string{"ConcatQuery1"}}

	// nombre_funcion := "query1YearAndAmount"
	//map con key de nombre de la funcion y clave tupla de cola input y cola output

	rconn, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	ch, _ := rconn.Channel()
	ch.QueueDeclare(
		"FilterMapper1YearAndAmount", // name
		false,                        // durable
		false,                        // delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,                          // arguments
	)
	msgs, _ := ch.Consume(
		"FilterMapper1YearAndAmount", // queue
		"",                           // consumer
		false,                        // auto-ack
		false,                        // exclusive
		false,                        // no-local
		false,                        // no-wait
		nil,                          // args
	)
	worker := concat.Concat{}
	var result []packet.Packet
	for message := range msgs {
		packet_reader := bytes.NewReader(message.Body)
		packet, _ := packet.DeserializePackage(packet_reader)
		fmt.Printf("HOLA ESTAS EN EL CONCAT %v\n", packet)
		result = append(result, worker.Process(packet)[0])
		fmt.Printf("HOLA YA PROCESASTE EL PAQUETE DEL CONCAT %v\n", result)
		if packet.IsEOF() {
			break
		}
	}
}
