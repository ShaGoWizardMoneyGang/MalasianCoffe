# MalasianCoffe

## Docker custom images
El repositorio usa un par de imagenes de docker custom. Todas estan en el directorio: `docker_images/`.

Para construirlas, se puede llamar a la directiva:
```
make docker-build-all
```
Va a crear una imagen por cada sub-directorio en `docker_images/` llamada igual que el nombre del subdirectorio.

## Links
- [Documentacion RabbitMQ](https://pkg.go.dev/github.com/rabbitmq/amqp091-go)
- [Graceful shuthdown RabbitMQ](fastfoto.net/posts/rabbitmq-work-queue-graceful-shutdown-with-timeout/)

# Comando para correr con docker
```bash
make build && docker compose -f docker-compose.yml up -d
```

# Comando para correr con docker (cambiar nombre de query cuando requiera)
```bash
make build && docker compose -f ./compose-files/queryN/queryN.yml up -d
```

# Comando para correr tests sobre output (RED => reducido, cuando tengamos full hay que correrlo con FULL)
```bash
make test-outputs-reduced
```
