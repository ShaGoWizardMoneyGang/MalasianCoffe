// Where the implementation for MessageMiddlewareExchange lives
package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeOptions struct {
	// Donde hablo con rabbit
	daemonAddress string
	routeKeys     []string
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
		daemonAddress: "amqp://guest:guest@localhost:5672/",
		routeKeys:     []string{"info"},
	}
}

func CreateExchange(name string, options ExchangeOptions) (*MessageMiddlewareExchange, error) {
	conn, err := amqp.Dial(options.daemonAddress)
	if err != nil {
		return nil, fmt.Errorf(`failed to connect to RabbitMQ: %w. Is the daemon active?
		Try running:
		
		sudo systemctl rabbitmq start 
		or
		sudo rc-service start rabbitmq`, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to the channel: %w", err)
	}

	err = ch.ExchangeDeclare(
		name,
		"direct", // porque si
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %w", err)
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	// TODO: Reemplazar por algo del estilo del debug_assert! en rust
	if err != nil {
		return nil, fmt.Errorf("Failed to declare queue: %w", err)
	}

	for _, key := range options.routeKeys {
		err := ch.QueueBind(
			q.Name, //cola anonima
			key,    // key, puedo tener mas de una por eso el for
			name,   // nombre del exchange
			false,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to bind queue: %w", err)
		}
	}

	// Channel es donde van a llegar los mensajes a la cola
	consumeChannel, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local/fix fmt.Errorf call has arguments but no formatting directives
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to consume queue: %w", err)
	}

	// Aca obtenemos channel
	return &MessageMiddlewareExchange{
		exchangeName:   name,
		routeKeys:      options.routeKeys,
		channel:        ch,
		consumeChannel: &consumeChannel,
	}, nil

}

// Interfaz MessageMiddleware

/*
Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
cada mensaje de datos o de control.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (q *MessageMiddlewareExchange) StartConsuming() (messageQueue MessageQueue, error MessageMiddlewareError) {
	return &q.consumeChannel, 0
}

/*
Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
*/
func (q *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	err := (*q.channel).Cancel(q.queueName, false)
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return 0
}

/*
Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (q *MessageMiddlewareExchange) Send(message []byte) (error MessageMiddlewareError) {
	err := (*q.channel).Publish(
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	if err != nil {
		return MessageMiddlewareMessageError
	}
	return 0
}

/*
Se desconecta de la cola o exchange al que estaba conectado.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
*/
func (q *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	err := (*q.channel).Close()
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return 0
}

/*
Se fuerza la eliminación remota de la cola o exchange.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
*/
func (q *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	lost_messages, err := (*q.channel).QueueDelete(
		q.queueName,
		false,
		false,
		false,
	)

	// TODO: Cambiar por log
	fmt.Printf("Mensajes perdidos: %d\n", lost_messages)

	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0
}
