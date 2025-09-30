package main

import (
	"bytes"
	"net"
	// "os"

	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_answer"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
)

// Argumentos que recibe
// 1: Direccion de rabbit
func main() {
	// TODO: Hacer que escuche de todas las colas de salida
	// rabbit_addr := os.Args[1]
	// rconn, _ := amqp.Dial(rabbit_addr)

	// ch, _ := rconn.Channel()

	// // NOTE: Declare para asegurarme que existe
	// ch.QueueDeclare(
	// 	"salida-1",  // name
	// 	false, // durable
	// 	false, // delete when unused
	// 	false, // exclusive
	// 	false, // no-wait
	// 	nil,   // arguments
	// )
	queue, err := middleware.CreateQueue("SalidaQuery1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic("Couldn't create query 1 queu")
	}
	// TODO: Esto no esta bueno para el sender porque tiene que escuchar de mas
	// de una cola a la vez, onda regex.
	msgs, _ := queue.StartConsuming()
	for message := range *msgs {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		client_receiver := pkt.GetClientAddr()
		conn, err := net.Dial("tcp", client_receiver)
		if err != nil {
			panic(err)
		}

		// TODO: Como averiguo de que cola vino?
		pkt_answer := packetanswer.From(pkt, "Query 1")
		pkt_answer_b := pkt_answer.Serialize()

		network.SendToNetwork(conn, pkt_answer_b)
		err = message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
