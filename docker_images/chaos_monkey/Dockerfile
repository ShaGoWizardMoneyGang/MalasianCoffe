# Compilamos un binario
FROM golang:alpine AS builder

LABEL intermediateStageToBeDeleted=true

ENV CGO_ENABLED=0 \
    GARCH=amd64 \
    GOOS=linux \
    CC=musl-gcc

WORKDIR /build

RUN mkdir bin/

COPY system system
COPY go.mod go.mod
COPY go.sum go.sum

RUN go build -C system/chaos_monkey -o /build/bin/chaos_monkey -ldflags="-extldflags=-static"

FROM docker:latest

# Copiamos el binario del builder en el docker image de docker in docker
COPY --from=builder /build/bin/chaos_monkey /app/bin/chaos_monkey
RUN chmod +x /app/bin/chaos_monkey