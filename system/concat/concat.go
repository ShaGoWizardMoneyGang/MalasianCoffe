package main

import (
	"bytes"
	"fmt"
	"malasian_coffe/packets/packet"
	concat "malasian_coffe/system/concat/src"
	"malasian_coffe/system/middleware"
	"time"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {
	colaEntrada, err := middleware.CreateQueue("FilterMapper1YearAndAmount", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(FilterMapper1YearAndAmount): %w", err))
	}
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}

	// type Colas struct {
	// 	colaInput  []string
	// 	colaOutput []string
	// }
	// funcionesYColas := make(map[string]Colas)
	// funcionesYColas["query1234"] = Colas{colaInput: []string{"FilterMapper1YearAndAmount"}, colaOutput: []string{"Concat1"}}
	// // funcionesYColas["query2"] = Colas{colaInput: []string{"entrada-2"}, colaOutput: []string{"ConcatQuery1"}}

	// // nombre_funcion := "query1YearAndAmount"
	// //map con key de nombre de la funcion y clave tupla de cola input y cola output

	// rabbit_addr := os.Args[1]
	// rconn, err := amqp.Dial("amqp://guest:guest@" + rabbit_addr + "/")
	// if err != nil {
	// 	panic(fmt.Errorf(`failed to rconnect to RabbitMQ: %w. Is the daemon active?
	// 	Try running:

	// 	sudo systemctl start rabbitmq
	// 	or
	// 	sudo rc-service rabbitmq start`))
	// }
	// ch, _ := rconn.Channel()
	// ch.QueueDeclare(
	// 	"FilterMapper1YearAndAmount", // name
	// 	false,                        // durable
	// 	false,                        // delete when unused
	// 	false,                        // exclusive
	// 	false,                        // no-wait
	// 	nil,                          // arguments
	// )
	// msgs, _ := ch.Consume(
	// 	"FilterMapper1YearAndAmount", // queue
	// 	"",                           // consumer
	// 	false,                        // auto-ack
	// 	false,                        // exclusive
	// 	false,                        // no-local
	// 	false,                        // no-wait
	// 	nil,                          // args
	// )
	colaSalida, err := middleware.CreateQueue("SalidaQuery1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(FilterMapper1YearAndAmount): %w", err))
	}
	// defer colaSalida.Close()

	worker := concat.Concat{}
	var result []packet.Packet
	for message := range *msgQueue {
		packet_reader := bytes.NewReader(message.Body)
		packet, _ := packet.DeserializePackage(packet_reader)
		result = worker.Process(packet)


		err := message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
		if len(result) != 0 {
			// NOTE: Solo para debug
			println("Envio al sender")
			time.Sleep(10 * time.Second)
			for _, pkt := range result {
				fmt.Printf("%v \n", pkt)
				err := colaSalida.Send(pkt.Serialize())
				println(err)
			}
		}
	}
}
