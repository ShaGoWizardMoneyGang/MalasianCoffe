
FROM ubuntu:latest

RUN useradd -ms /bin/bash user

RUN mkdir -p app && chown user:users app

RUN mkdir -p app/packet_receiver && chown user:users app/packet_receiver

RUN mkdir -p app/packet_receiver/tmp && chown user:users app/packet_receiver/tmp
