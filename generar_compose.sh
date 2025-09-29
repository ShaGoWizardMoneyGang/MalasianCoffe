#!/bin/bash
echo $1 $2
file_name=$1
client_number=$2

cat >$file_name <<EOL
name: tp1
services:
  server:
    container_name: server
    image: golang:tip-trixie
    entrypoint: go run /app/system/system.go 0.0.0.0:9091 rabbitmq:5672
    environment: #not in use, just for reference
    - LISTEN_ADDR=0.0.0.0:9091
    - RABBIT_ADDR=rabbitmq:5672
    networks:
      - testing_net
    volumes:
      - ./system:/system 
EOL

cat >>$file_name <<EOL

  gateway:
    container_name: gateway
    image: golang:tip-trixie
    entrypoint: go run /app/gateway/gateway.go localhost:9090 localhost:9091
    networks:
      - testing_net
    volumes:
      - ./gateway:/gateway

EOL


cat >>$file_name <<EOL

  rabbitmq:
    container_name: rabbitmq_test
    image: rabbitmq:4.1.4-management
    ports:
      - "5673:5672"   # AMQP (container 5672 → host 5673)
      - "15673:15672" # Management UI (container 15672 → host 15673)
    networks:
      - testing_net
EOL

for i in $(seq 1 $client_number);
do
cat >>$file_name <<EOL

  client${i}:
    container_name: client${i}
    image: golang:tip-trixie
    entrypoint: go run /app/client/client.go
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./dataset:/dataset
      - ./client:/client

EOL
done

cat >>${file_name} << EOL

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOL