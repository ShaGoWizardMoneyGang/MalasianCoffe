package colas

import (
	"bytes"
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"

	amqp "github.com/rabbitmq/amqp091-go"
)

type OutBoundMessage struct {
	Packet packet.Packet
	ColaSalida middleware.MessageMiddleware
}

type PacketMessage struct {
	Packet packet.Packet
	Message amqp.Delivery
}

// Wrapper function a las colas para hacerlo mas amigable

func ConsumeInput(colaEntrada *middleware.MessageMiddlewareQueue) middleware.ConsumeChannel {
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	return msgQueue
}

func InstanceQueueRouted(exchangeName string, rabbitAddr string, routingKey string) *middleware.MessageMiddlewareQueue {
	cola, err := middleware.CreateQueueUnderExchange(exchangeName, middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)}, routingKey)
	if err != nil {
		panic(fmt.Errorf("CreateQueue: %w", err))
	}

	return cola
}

func InstanceQueue(inputQueueName string, rabbitAddr string) *middleware.MessageMiddlewareQueue {
	cola, err := middleware.CreateQueue(inputQueueName, middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
	if err != nil {
		panic(fmt.Errorf("CreateQueue(%s): %w", inputQueueName, err))
	}
	return cola
}

func InstanceExchange(exchangeName string, rabbitAddr string, queueAmount uint64) *middleware.MessageMiddlewareExchange {
	exchange, err := middleware.CreateExchange(exchangeName, middleware.ExchangeOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr), QueueAmount: queueAmount})
	if err != nil {
		panic(fmt.Errorf("Failed to CreateExchange(%s): %w", exchangeName, err))
	}
	return exchange
}

func InputQueue(input *middleware.MessageMiddlewareQueue, inputChannel chan<- PacketMessage) {
	colasEntrada := input

	messages := ConsumeInput(colasEntrada)
	for message := range *messages {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		err := message.Ack(false)
		if err != nil {
			bitacora.Error(fmt.Errorf("Could not ack, %w", err).Error())
		}

		packet_message := PacketMessage {
			Packet: pkt,
			Message: message,
		}

		inputChannel <- packet_message
	}
}
