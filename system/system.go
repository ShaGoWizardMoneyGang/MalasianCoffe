package main

import (
	"fmt"

	"malasian_coffe/packet"

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
		"entrada-1", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	/*	rabbit_addr := os.Args[1]

		list, err := net.Listen("tcp", rabbit_addr)
		if err != nil {
			panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
		}
		conn, _ := list.Accept()*/

	transactionsRaw := []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54"}
	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionsRaw)[0]

	fmt.Printf("%v\n", pkt)
	ch.Publish("", "entrada-1", false, false,
		amqp.Publishing{
			// DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        pkt.Serialize(),
		})

	/*for {
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
	}*/
}
