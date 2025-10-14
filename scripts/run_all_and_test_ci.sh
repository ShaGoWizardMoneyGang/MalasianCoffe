#!/bin/bash
set -euo pipefail

make download-reduced-dataset

make generate-compose

# Levantar todos los servicios en detached mode
make docker-multi

# Seguir logs de client1 en tiempo real
docker logs -f client1 &
LOG_PID=$!

# Esperar a que client1 termine
make docker-wait

# Terminar seguimiento de logs
kill $LOG_PID || true

# Ejecutar tests
bash scripts/test_outputs.sh $1
