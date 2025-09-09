# MalasianCoffe

## RabbitMQ en Golang

### 1° Implementacion: SEND/RECEIVE

Dentro del directorio ```send-receiver```.

En una terminal:

```go run send.go```

En otra:

```go run receiver/receive.go```

### 2° Implementacion: WORK QUEUES

Dentro del directorio ```work-queues```.

Puedo levantar n workers que esperan mensajes de la queue:

```go run worker/worker.go```

Puedo enviar mensajes que recibira alguno de los workers:

```go run new_task.go hello```

### 3° Implementacion: PUBLISH-SUSCRIBE

Esta implementacion realiza un broadcast de todos los mensajes a todos los consumidores.

Puedo recibir logs y mandarlos a un archivo:

```go run receive_logs.go &> logs_from_rabbit.log```

Puedo enviar mensajes que se loguean:

```go run emit_log.go```

### 4° Implementacion: ROUTING

Ahora podemos "enrutar" a los mensajes para que los reciba un determinado consumidor.

Solamente quiero guardar los logs de error y warning:

```go run receive_logs_direct.go warning error &> logs_from_rabbit.log```

Quiero ver todos los mensajes:

```go run receive_logs_direct.go info warning error```

Puedo emitir un mensaje del tipo error:

```go run emit_log_direct.go info "Run. Run. Or it will explode."```