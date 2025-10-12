// Where the implementation for MessageMiddlewareExchange lives
package middleware

import (
	"fmt"
	"malasian_coffe/packets/packet"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeOptions struct {
	// Donde hablo con rabbit
	DaemonAddress string
	QueueAmount   uint64
	// routeKeys     []string
	/*	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	*/
}

func OptionsDefault() ExchangeOptions {
	return ExchangeOptions{
		DaemonAddress: "amqp://guest:guest@localhost:5672/",
		// routeKeys:     []string{"info"},
	}
}

func CreateExchange(name string, options ExchangeOptions) (*MessageMiddlewareExchange, error) {
	conn, err := amqp.Dial(options.DaemonAddress)
	if err != nil {
		return nil, fmt.Errorf(`failed to connect to RabbitMQ: %w. Is the daemon active?
		Try running:

		sudo systemctl start rabbitmq
		or
		sudo rc-service rabbitmq start`, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to the channel: %w", err)
	}

	err = ch.ExchangeDeclare(
		name,
		"direct", // Queremos que el exchange envie pkts a ciertas colas nomas.
		false, // durable
		false, // auto-deleted
		false, // internal
		false, // no wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %w", err)
	}

	fmt.Printf("[CreateExchange] Exchange %s declarado\n", name)

	consumerTag := "ctag-" + name + "-" + uuid.New().String()
	fmt.Printf("[CreateExchange] ConsumerTag asignado: %s\n", consumerTag)

	// Aca obtenemos channel
	return &MessageMiddlewareExchange{
		exchangeName:   name,
		QueueAmount: options.QueueAmount,
		channel:        ch,
		consumerTag: consumerTag,
	}, nil

}

// Interfaz MessageMiddleware

/*
Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
cada mensaje de datos o de control.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (q *MessageMiddlewareExchange) StartConsuming() (messageQueue ConsumeChannel, error MessageMiddlewareError) {
	panic("Tried to listen from an exchange")
}

/*
Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
*/
func (e *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	panic("Tried to stop listen from an exchange")
}

/*
Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
// func (e *MessageMiddlewareExchange) Send(message []byte) (error MessageMiddlewareError) {
func (e *MessageMiddlewareExchange) Send(pkt packet.Packet) (error MessageMiddlewareError) {
	message    := pkt.Serialize()
	routingKey := packet.GenerateRoutingKey(pkt, e.QueueAmount)

	err := (*e.channel).Publish(
		e.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			// DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		panic(fmt.Errorf("failed to send message: %w", err))
		return MessageMiddlewareMessageError
	}

	return 0
}

/*
Se desconecta de la cola o exchange al que estaba conectado.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
*/
func (e *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	err := (*e.channel).Close()
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return 0
}

/*
Se fuerza la eliminación remota de la cola o exchange.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
*/
func (e *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	// claro aca el tema es: tengo que borrar la cola tambien??? si la tengo que borrar
	// tendria que guardarla en algun lado para poder obtener el name.
	err := (*e.channel).ExchangeDelete(
		e.exchangeName,
		true,
		false,
	)

	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0
}
