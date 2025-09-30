package main

import (
	"bytes"
	"fmt"
	"net"

	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_answer"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
)

// Argumentos que recibe
// 1: Direccion de rabbit
func main() {
	// TODO: Esto no esta bueno para el sender porque tiene que escuchar de mas
	// de una cola a la vez, onda regex.
	queue, err := middleware.CreateQueue("SalidaQuery1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic("Couldn't create query 1 queu")
	}
	msgs, err_2 := queue.StartConsuming()
	if err_2 != 0 {
		panic("Couldn't start consuming queue 2")
	}
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
