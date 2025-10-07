#!/bin/bash
set -euo pipefail

make build

docker compose -f docker-compose-ci.yml up -d

# Espera a que client1 exista
while ! docker ps -q -f name=client1 | grep -q .; do
  printf "Esperando a que client1 se cree...\n" >&2
  sleep 1
done

# Seguir logs de client1 en background
docker logs -f client1 &
LOG_PID=$!

# Espera a que client1 termine
docker wait client1

# Termina el seguimiento de logs
kill $LOG_PID || true

bash test_outputs.sh RED
