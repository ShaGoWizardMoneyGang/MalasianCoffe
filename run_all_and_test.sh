#!/bin/bash
docker compose -f docker-compose.yml down -v --remove-orphans
rm out/Query1.csv out/Query2b.csv out/Query2a.csv out/Query3.csv out/Query4.csv

clear

make build

docker compose -f docker-compose.yml up -d

make run-client

bash test_outputs.sh RED