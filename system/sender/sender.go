package main

import (
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Argumentos que recibe
// 1: Direccion de rabbit
func main() {
	// TODO: Hacer que escuche de todas las colas de salida
	rabbit_addr := os.Args[1]
	rconn, _ := amqp.Dial(rabbit_addr)

	ch, _ := rconn.Channel()

	// NOTE: Declare para asegurarme que existe
	ch.QueueDeclare(
		"salida-1",  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}
