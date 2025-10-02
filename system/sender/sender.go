package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

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
	rabbit_addr := os.Args[1]

	// Esto tiene un nombre del Query1
	numeroQuery := os.Args[2]
	if numeroQuery == "" {
		panic("No se le paso Query al sender, tiene que ser algo del estilo make run-sender RUN_FUNCTION=Query1")
	}
	queue, err := middleware.CreateQueue("Salida" + numeroQuery, middleware.ChannelOptions {DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	if err != nil {
		panic("Couldn't create " + numeroQuery)
	}
	msgs, err_2 := queue.StartConsuming()
	if err_2 != 0 {
		  panic("Couldn't start consuming queue 2")
	   }
	   // NOTE: Este sleep lo pongo porque si el dataset es corto, el cliente envia todo y no le da tiempo a crear un socket
	   time.Sleep(10 * time.Second)
	for message := range *msgs {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		client_receiver := pkt.GetClientAddr()
		conn, err := net.Dial("tcp", client_receiver)
		if err != nil {
			panic(err)
		}

		// TODO: Como averiguo de que cola vino?
		pkt_answer := packetanswer.From(pkt, numeroQuery)
		pkt_answer_b := pkt_answer.Serialize()

		slog.Info("Sending answer packet back to client")
		network.SendToNetwork(conn, pkt_answer_b)
		err = message.Ack(false)
		if err != nil {
			panic(fmt.Errorf("Could not ack, %w", err))
		}
	}
}
