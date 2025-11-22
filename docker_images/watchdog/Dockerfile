# Compilamos un binario
FROM golang:alpine AS builder

LABEL intermediateStageToBeDeleted=true

ENV CGO_ENABLED=0 \
    GARCH=amd64 \
    GOOS=linux \
    CC=musl-gcc

WORKDIR /build

RUN mkdir bin/

COPY system/watchdog system/watchdog
COPY go.mod go.mod
COPY go.sum go.sum

RUN go build -C system/watchdog -o /build/bin/watchdog -ldflags="-extldflags=-static" -tags osusergo,netgo

FROM docker:latest

# Copiamos el binario del builder en el docker image de docker in docker
COPY --from=builder /build/bin/watchdog /app/bin/watchdog
