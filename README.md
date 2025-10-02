# MalasianCoffe

## Links
- [Documentacion RabbitMQ](https://pkg.go.dev/github.com/rabbitmq/amqp091-go)
- [Graceful shuthdown RabbitMQ](fastfoto.net/posts/rabbitmq-work-queue-graceful-shutdown-with-timeout/)

# Comando para correr con docker (cambiar nombre de query cuando requiera)
```bash
make build && docker compose -f ./compose-files/queryN/queryN.yml up -d
```