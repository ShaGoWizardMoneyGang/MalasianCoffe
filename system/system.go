package main

import (
	"bytes"
	"fmt"
	"net"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/network"

)

func main() {
	rabbit_addr := os.Args[2]
	rconn, err := amqp.Dial("amqp://guest:guest@" + rabbit_addr + "/")

	if err != nil {
		panic(fmt.Errorf(`failed to rconnect to RabbitMQ. Is the daemon active?
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
		"DataQuery1", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	//listen_addr
	listen_addr := os.Args[1]

	list, err := net.Listen("tcp", listen_addr)
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

		// TODO: esto esta hardcodeado asi porque es para la query 1.
		// Aca deberia haber un switch que lo envie a la queue correspondiente
		if packet.GetDirID() != "3"{
			continue
		}
		ch.Publish("", "DataQuery1", false, false,
			amqp.Publishing{
				// DeliveryMode: amqp.Persistent,
				ContentType: "text/plain",
				Body:        packet.Serialize(),
			})
	}
}
