package colas

import (
	"bytes"
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
)

// Wrapper function a las colas para hacerlo mas amigable

func ConsumeInput(colaEntrada *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	return msgQueue
}

func InstanceQueue(inputQueueName string, rabbitAddr string) *middleware.MessageMiddlewareQueue {
	cola, err := middleware.CreateQueue(inputQueueName, middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", inputQueueName, err))
	}
	return cola
}

func InputQueue(input *middleware.MessageMiddlewareQueue, inputChannel chan<- packet.Packet) {
	colasEntrada := input

	messages := ConsumeInput(colasEntrada)
	for message := range *messages {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		err := message.Ack(false)
		if err != nil {
			bitacora.Error(fmt.Errorf("Could not ack, %w", err).Error())
		}

		inputChannel <- pkt
	}
}
