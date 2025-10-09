#!/bin/bash
docker compose -f docker-compose-gen.yml down -v --remove-orphans
rm out/Query1.csv out/Query2b.csv out/Query2a.csv out/Query3.csv out/Query4.csv

python3 generar_compose.py

make build && docker compose -f docker-compose-gen.yml up -d

make run-client

bash test_outputs.sh RED