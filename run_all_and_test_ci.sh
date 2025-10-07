#!/bin/bash
make build

docker compose -f docker-compose-ci.yml up -d

while ! docker ps -q -f name=client1 | grep -q .; do
  echo "Esperando a que client1 se cree..."
  sleep 1
done

docker wait client1

bash test_outputs.sh RED