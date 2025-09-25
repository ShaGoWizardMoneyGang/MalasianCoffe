// Where the implementation for MessageMiddlewareExchange lives
package middleware

import (
	"fmt"

	"github.com/google/uuid"
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
	fmt.Printf("[CreateExchange] Exchange %s declarado, cola %s creada\n", name, q.Name)

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
		fmt.Printf("[CreateExchange] Cola %s bindeada a exchange %s con routingKey=%s\n", q.Name, name, key)
	}
	consumerTag := "ctag-" + name + "-" + uuid.New().String()
	fmt.Printf("[CreateExchange] ConsumerTag asignado: %s\n", consumerTag)

	// Channel es donde van a llegar los mensajes a la cola
	consumeChannel, err := ch.Consume(
		q.Name,      // queue
		consumerTag, // consumer ES ACA LA TECA ESTO ME LO TENGO QUE GUARDAR
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local/fix fmt.Errorf call has arguments but no formatting directives
		false,       // no-wait
		nil,         // args
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
		consumerTag:    consumerTag,
	}, nil

}

// Interfaz MessageMiddleware

/*
Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
cada mensaje de datos o de control.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (_q *MessageMiddlewareExchange) StartConsuming() (messageQueue MessageQueue, error MessageMiddlewareError) {
	return &_q.consumeChannel, 0
}

/*
Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
*/
func (_q *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	// Aca necesito de alguna manera obtener que quiero cancelar
	// el nombre del exchange no es valido dentro del cancel
	err := (*_q.channel).Cancel(_q.consumerTag, false)
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	fmt.Printf("[StopConsuming] Cancelando consumer %s...\n", _q.consumerTag)

	return 0
}

/*
Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (_q *MessageMiddlewareExchange) Send(message []byte) (error MessageMiddlewareError) {
	for _, key := range _q.routeKeys {
		fmt.Printf("[Send] Publicando mensaje '%s' en exchange=%s, routingKey=%s\n",
			string(message), _q.exchangeName, key)
		err := (*_q.channel).Publish(
			_q.exchangeName, // exchange
			key,             // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        message,
			})
		if err != nil {
			return MessageMiddlewareMessageError
		}
	}
	return 0
}

/*
Se desconecta de la cola o exchange al que estaba conectado.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
*/
func (_q *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	err := (*_q.channel).Close()
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return 0
}

/*
Se fuerza la eliminación remota de la cola o exchange.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
*/
func (_q *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	// claro aca el tema es: tengo que borrar la cola tambien??? si la tengo que borrar
	// tendria que guardarla en algun lado para poder obtener el name.
	err := (*_q.channel).ExchangeDelete(
		_q.exchangeName,
		true,
		false,
	)

	if err != nil {
		return MessageMiddlewareDeleteError
	}

	return 0
}
