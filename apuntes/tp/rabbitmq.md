# RabbitMQ
## Queues
- Los mensajes se guardan en queues, pero el **producer por defecto distribuye todas las tareas a todos los clientes disponibles** (no bueno para nuestro diseno). 
    - Seguir leyendo para abajo la parte de **prefetch**.
- En go se declara con `QueueDeclare`. Recibe:
    - Nombre
    - Durable
    - Delete when unused
    - Exclusive
    - No-wait
    - Arguments
- Crear queues es idempotente. Solo se crean si no existen. **Va por nombre**.
- Vos "declaras" la queue tanto en el cliente como en el servidor para asegurarte que la queue existe
    - Esto puede ser que no nos aplique a nosotros si garantizamos que el nodo rabbit siempre se levanta primero
- Vos empezas a recibir los mensajes con `amqp::Consume`. Como es asincronico, lo tenes que dejar en un loop
- RabbitMQ ya desde el vamos soporta ACK (agradecido con el de arriba)
    - RabbitMQ si no recibe ACK de un mensaje, re-queue-ea el mensaje.
    - El timeout es configurable.
    - Pones `autoack = false` y despues envias el ACK.
- Vos podes marcar **las queues y los mensajes** como durables para que RabbitMQ las guarde. Si Rabbit crashea, otro proceso toma la tarea.
- **Prefetch**: Dice cuantos mensajes puede tener un consumer para que el producer le envie un mensaje
    - Nosotros potencialmente vamos a querer prefetch = 1 para que solo tenga 1 tarea en cada momento cada nodo.
    - La queue podria llegar a llenarse si todos los clientes estan ocupados. Estrategia de peso?
- **Exchanges**: Paso intermedio (opcional) entre el producer y consumer. Determina que hacer con le paquete (a quien enviarlo o si dropearlo).

