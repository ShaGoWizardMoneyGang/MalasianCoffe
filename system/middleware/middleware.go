package middleware

import (
	"malasian_coffe/packets/packet"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareChannel *amqp.Channel
type ConsumeChannel *<-chan amqp.Delivery

type MessageMiddlewareError int

const (
	MessageMiddlewareMessageError MessageMiddlewareError = iota + 1
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError
)

type MessageMiddlewareQueue struct {
	queueName      string
	channel        MiddlewareChannel
	consumerTag    string
}

type MessageMiddlewareExchange struct {
	exchangeName   string
	// Cantidad de colas destino a las que el exchange le va a hablar.
	QueueAmount   uint64
	channel        MiddlewareChannel
	consumerTag    string
}

type MessageQueue *ConsumeChannel

type onMessageCallback func(consumeChannel ConsumeChannel, done chan error)

// Puede especificarse un tipo más específico para T si se desea
type MessageMiddleware interface {
	/*
	   Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
	   cada mensaje de datos o de control.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	StartConsuming() (messageQueue ConsumeChannel, error MessageMiddlewareError)

	/*
	   Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	   no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	*/
	StopConsuming() (error MessageMiddlewareError)

	/*
	   Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	Send(pkt packet.Packet) (error MessageMiddlewareError)

	/*
	   Se desconecta de la cola o exchange al que estaba conectado.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	*/
	Close() (error MessageMiddlewareError)

	/*
	   Se fuerza la eliminación remota de la cola o exchange.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	*/
	Delete() (error MessageMiddlewareError)
}
