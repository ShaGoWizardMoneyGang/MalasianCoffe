package main

import (
	"bytes"
	"fmt"
	"malasian_coffe/packet"
	filter_mapper "malasian_coffe/system/filter_mapper/src"

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
	funcionesYColas["query1YearAndAmount"] = Colas{colaInput: []string{"entrada-1"}, colaOutput: []string{"salida-1"}}
	// funcionesYColas["query2"] = Colas{colaInput: []string{"entrada-2"}, colaOutput: []string{"salida-2"}}

	// nombre_funcion := "query1YearAndAmount"
	//map con key de nombre de la funcion y clave tupla de cola input y cola output

	rconn, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	ch, _ := rconn.Channel()
	ch.QueueDeclare(
		"entrada-1", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	msgs, _ := ch.Consume(
		"entrada-1", // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	worker := filter_mapper.FilterMapper{}
	var result []packet.Packet
	for message := range msgs {
		packet_reader := bytes.NewReader(message.Body)
		packet, _ := packet.DeserializePackage(packet_reader)
		fmt.Printf("HOLA ESTAS EN EL FILTER MAPPER %v\n", packet)
		result = append(result, worker.Process(packet, "query1YearAndAmount"))
		fmt.Printf("HOLA YA PROCESASTE EL PAQUETE %v\n", result)
		break
	}
	ch.QueueDeclare(
		"salida-1", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	fmt.Printf("%v\n", result)
	for _, packet := range result {
		ch.Publish("", "salida-1", false, false,
			amqp.Publishing{
				// DeliveryMode: amqp.Persistent,
				ContentType: "text/plain",
				Body:        packet.Serialize(),
			})
	}
}
