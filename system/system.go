package main

import (
	"fmt"
	"bytes"
	"net"
	"os"

	"malasian_coffe/packet"

	"malasian_coffe/utils/network"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rconn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		panic(fmt.Errorf(`failed to rconnect to RabbitMQ: %w. Is the daemon active?
		Try running:

		sudo systemctl start rabbitmq
		or
		sudo rc-service rabbitmq start`))
	}
	ch, err := rconn.Channel()
	if err != nil {
		panic(fmt.Errorf("Failed to connect to the channel: %w", err))
	}
	ch.QueueDeclare(
		"prueba",  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	rabbit_addr := os.Args[1]

	list, err := net.Listen("tcp", rabbit_addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
	}
	conn, _ := list.Accept()

	for {
		packet_b, err := network.ReceiveFromNetwork(conn)
		packet_reader := bytes.NewReader(packet_b)
		if err != nil {
			panic(err)
		}
		packet, err := packet.DeserializePackage(packet_reader)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", packet)
		ch.Publish("", "prueba", false, false,
		amqp.Publishing{
			// DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        packet.Serialize(),
		})
	}
}
