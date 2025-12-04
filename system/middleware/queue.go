// Where the implementation for MessageMiddlewareQueue lives
package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/uuid"
)

type ChannelOptions struct {
	// Donde hablo con rabbit
	DaemonAddress string
	/*	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	*/
}

func ChannelOptionsDefault() ChannelOptions {
	return ChannelOptions{
		DaemonAddress: "amqp://guest:guest@localhost:5672/",
	}
}

func CreateQueueUnderExchange(exchangeName string, options ChannelOptions, routingKey string) (*MessageMiddlewareQueue, error) {
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

	queueName := exchangeName + "-" + routingKey

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct", // Queremos que el exchange envie pkts a ciertas colas nomas.
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no wait
		nil,
	)

	ch.Qos(1, 0, false)

	if err != nil {
		return nil, fmt.Errorf("Failed to declare exchange: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to declareQueue: %w", err)
	}

	ch.Qos(1, 0, false)

	err = ch.QueueBind(
		q.Name,       // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to bind queue to exchange: %w", err)
	}

	consumerTag := "ctag-" + q.Name + "-" + uuid.GenerateUUID()
	return &MessageMiddlewareQueue{
		queueName:   q.Name,
		channel:     ch,
		consumerTag: consumerTag,
	}, nil
}

func CreateQueue(name string, options ChannelOptions) (*MessageMiddlewareQueue, error) {

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
	err = ch.Confirm(false)
	if err != nil {
		panic("Failed to create channel")
	}

	q, err := ch.QueueDeclare(
		name,  // name
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
	if q.Name != name {
		fmt.Printf("Nombre de la cola: %s recibida por parametro y nombre de la cola creada: %s", name, q.Name)
		panic("Queue name does not match received name")
	}
	consumerTag := "ctag-" + name + "-" + uuid.GenerateUUID()
	// Channel es donde van a llegar los mensajes a la cola

	confirmation := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Aca obtenemos channel
	return &MessageMiddlewareQueue{
		queueName:   name,
		channel:     ch,
		consumerTag: consumerTag,

		confirmChannel: confirmation,
	}, nil

}

// Interfaz MessageMiddleware

/*
Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
cada mensaje de datos o de control.
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
*/
func (q *MessageMiddlewareQueue) StartConsuming() (messageQueue ConsumeChannel, error MessageMiddlewareError) {
	consumeChannel, err := (*q.channel).Consume(
		q.queueName, // queue
		"",          // consumer

		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⣤⡴⠶⠾⠿⠛⠟⠻⠿⠿⠶⠶⣤⣤⣄⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣴⠶⠟⠋⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠙⠛⠷⢶⣦⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣴⠾⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠲⠶⢾⣿⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⡏⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡄⠙⢻⣶⡄⠀⠀⠀⠀⠀⠀⠀⠀
		// ================================ NO TOCAR ================================== // ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣶⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⣦⣄⠀⠀⠀⠀⠀⠀
		/* LOS ACKS SON FUNDAMENTALES en RABBIT */ // ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣼⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠈⠛⣷⡄⠀⠀⠀⠀
		/* SI NECESITAS SACARLO PARA DEBUGGEAR, DEBBUGEA DE OTRA FORMA */ // ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⡿⠁⠀⠀⠀⣄⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⠀⠀⠀⠀⠈⢿⣆⠀⠀⠀
		false,                                                            // auto-ack ENSERIO NO TOCAR ESTE BOOLEANO                               // ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣾⠏⠀⠀⠀⠀⠀⠈⠛⢷⣤⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣴⡿⠛⠀⠀⠀⠀⠀⠀⠀⢿⡆⠀⠀
		// NO LO MIRES DEMASIADO TAMPOCO                                                // ⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠛⠷⣦⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⠾⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⠀⠀
		// ================================ NO TOCAR ================================== // ⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⣷⣿⣿⣾⢿⣇⠀⠀⠀⢀⣠⣤⣤⣤⣬⣙⣿⣦⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⠟⣋⣁⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⡇⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⣾⣟⣛⣉⣽⠉⢻⣆⣠⡶⠟⢛⣩⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣾⣿⣿⣿⣿⣿⣿⣿⣟⠻⠷⣦⣄⠀⠀⠀⠀⠀⠀⢸⣷⠀
		// ⠀⠀⠀⠀⠀⣠⣤⣶⣟⣋⡭⣿⠃⢠⡿⢙⠋⠀⣠⣿⣿⣿⣿⣿⣿⣿⣿⣿⠣⣿⣿⡄⠀⠀⠀⠀⠀⠀⠀⣿⡏⢻⣿⣿⣿⣿⣿⣿⣿⣿⣷⣄⠀⠙⢷⡄⠀⠀⠀⠀⠀⣿⠀
		// ⠀⠀⠀⠀⢰⣿⣿⣧⣤⣻⡿⠁⣠⡿⠁⢸⣇⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⣿⣿⣿⡄⠀⠀⠀⠀⠀⣼⣿⠇⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⡀⠈⢿⡆⠀⠀⠀⠀⣿⡆
		// ⠀⠀⢀⣴⣿⣽⣯⣭⣍⠍⠀⣰⡿⠁⢠⡾⠿⣮⠝⠛⠻⣿⣿⣿⣿⣿⣯⠏⠀⠻⣿⣿⣿⣄⢠⣤⠤⣾⠿⠟⠀⠈⠳⠿⠿⣿⣿⣿⣿⣿⣿⣿⠿⠃⠀⠸⣷⠀⠀⠀⠀⢹⣇
		// ⠀⣴⣿⣿⣿⣶⣾⣿⣻⣿⣾⡿⠁⠀⢸⡿⣭⣿⣿⣶⣃⣀⣙⣻⣾⣂⣀⠀⠀⠀⣀⣀⣹⣿⠠⣤⢤⣦⣤⣀⠀⠀⢠⣤⣤⣶⣶⣿⣿⠋⠉⣀⣠⣤⡄⠀⣿⠃⠀⠀⠀⣸⣯
		// ⢸⣿⣿⣟⣿⣿⣿⣦⡹⣯⣿⡇⠀⣤⣼⢃⣿⣿⣿⣿⣟⣿⡙⠀⠉⠻⣿⣧⠀⣴⣿⣿⣿⡇⠀⠀⠐⣿⣿⣿⠆⢸⣿⣽⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⢰⣿⠲⡾⣛⠻⢛⡷
		// ⢸⣿⡟⣾⣿⣿⣿⣿⣷⢹⣿⣷⠀⠘⣿⣿⣿⣿⣿⣿⡗⠀⠀⠀⠀⢀⣿⣿⠀⣿⣿⣿⣿⠁⠀⠀⠀⣿⣿⣿⡇⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣾⠃⢠⠕⢡⠣⠰⣟
		// ⠈⣿⣿⣝⣿⣿⣿⡿⢏⣿⢿⣿⠃⣸⠟⠿⠛⠿⣿⣿⡗⠀⣀⣀⣀⣼⣿⣿⣯⣿⣿⠟⠃⠀⠀⠀⠀⠘⠿⣿⣿⣽⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠃⣰⢋⡜⢆⢣⢘⣯
		// ⠀⠈⠻⣿⣷⣶⣶⣷⠿⣿⣿⠃⢰⣏⠈⠉⠉⠙⠛⣿⣯⠐⠀⠀⢹⣿⣿⠿⠟⠉⠀⠀⠀⣀⣀⣀⡀⠀⠀⠈⠙⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠋⣠⢞⡓⢭⡘⡌⠆⣸⡧
		// ⠀⠀⠀⢹⣿⣫⣭⣭⡻⢿⣧⢠⣿⣯⡴⣾⣅⣀⣴⣟⠻⣬⣓⣤⣿⠉⠀⠀⠀⠀⣠⣾⠟⠛⠛⠛⢿⣶⡄⠀⠀⠀⠀⠈⠙⠛⠛⠛⠛⠉⣰⠶⡒⣍⠲⢬⡘⢆⡳⢈⠄⣿⠁
		// ⠀⠀⠀⠸⣿⣿⣿⣿⣿⣿⡿⣼⣿⡜⡟⠉⠉⠉⠱⠶⠥⣼⣟⠉⠀⠀⠀⠀⠀⠈⠋⠁⠀⠀⠀⠀⠀⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⢣⠱⣌⠚⡤⣙⢢⡱⠉⣼⡏⠀
		// ⠀⠀⠀⠀⣿⣧⣿⣿⣟⣼⡿⣿⢿⣿⠄⢀⣀⠀⠀⠀⠀⠸⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⡇⢧⠘⢧⢠⢃⠼⠃⣸⡿⠀⠀
		// ⠀⠀⠀⠀⢻⣯⣽⣿⣿⣭⣿⣿⡋⠉⡉⠬⣉⠏⡉⢀⣤⣾⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣢⠙⡆⢣⠞⠐⣼⠏⠀⠀⠀
		// ⠀⠀⠀⠀⠀⢿⣿⣿⣿⣿⣿⣿⣷⣧⡽⠧⠷⠶⠚⠋⠁⠀⣽⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣧⡜⢓⣨⠞⠁⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠸⢿⣿⣿⣿⣿⣿⣶⣶⠂⠀⣀⢐⡲⠆⠀⣠⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⠿⠋⠁⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠘⢿⣿⣿⣿⣿⣿⡾⣭⣳⣌⣣⣴⠶⠛⢷⡋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⣴⠶⠟⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠉⢿⣯⣯⣙⡉⢉⣉⡉⠁⣀⣀⣴⠟⠛⠳⠶⣦⣤⣤⣀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣤⣤⣶⠶⠛⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⠻⢿⣭⣥⣫⣟⣖⣰⠟⠁⠀⠀⠀⠀⠀⠀⠈⠉⠉⠙⠛⠛⠛⠛⠛⠛⠛⠛⠛⠉⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠉⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
		// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

		false, // exclusive
		false, // no-local/fix fmt.Errorf call has arguments but no formatting directives
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		panic(fmt.Errorf("Failed to register a consumer: %w", err))
		return nil, 1
	}
	return &consumeChannel, 0
}

/*
Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
*/
func (q *MessageMiddlewareQueue) StopConsuming() (error MessageMiddlewareError) {
	err := (*q.channel).Cancel(q.consumerTag, false) // LE PUSE EL CONSUMER TAG
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
func (q *MessageMiddlewareQueue) Send(pkt packet.Packet) (error MessageMiddlewareError) {

	message := pkt.Serialize()
	err := (*q.channel).Publish(
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			// DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		panic(fmt.Errorf("failed to send message: %w", err))
		return MessageMiddlewareMessageError
	}

	confirmation, ok := <-q.confirmChannel
	if !ok {
		panic(fmt.Errorf("failed to send message: %w", err))
	}
	if !confirmation.Ack {
		panic(fmt.Errorf("failed to send message: %w", err))
	}

	return 0
}

/*
Se desconecta de la cola o exchange al que estaba conectado.
Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
*/
func (q *MessageMiddlewareQueue) Close() (error MessageMiddlewareError) {
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
func (q *MessageMiddlewareQueue) Delete() (error MessageMiddlewareError) {
	// lost_messages, err := (*q.channel).QueueDelete(
	// 	q.queueName,
	// 	false,
	// 	false,
	// 	false,
	// )

	// // TODO: Cambiar por log
	// fmt.Printf("Mensajes perdidos: %d\n", lost_messages)

	// if err != nil {
	// 	return MessageMiddlewareDeleteError
	// }

	return 0
}
