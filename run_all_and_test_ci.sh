#!/bin/bash
set -euo pipefail

make build

make download-reduced-dataset

# Levantar todos los servicios en detached mode
docker compose -f docker-compose-ci.yml up -d

# Seguir logs de client1 en tiempo real
docker logs -f client1 &
LOG_PID=$!

# Esperar a que client1 termine
docker wait client1

# Terminar seguimiento de logs
kill $LOG_PID || true

# Ejecutar tests
bash test_outputs.sh RED
