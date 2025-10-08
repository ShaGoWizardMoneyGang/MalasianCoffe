#!/bin/bash
docker compose -f docker-compose.yml down -v --remove-orphans

python3 generar_compose.py

make build && docker compose -f docker-compose-gen.yml up -d