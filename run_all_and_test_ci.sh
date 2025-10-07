#!/bin/bash
make build

docker compose -f docker-compose-ci.yml up -d

docker wait client1