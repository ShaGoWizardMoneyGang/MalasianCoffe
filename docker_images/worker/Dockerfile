FROM ubuntu:latest

RUN useradd -ms /bin/bash user

RUN apt-get update && apt-get install -y tree

RUN mkdir -p app && chown user:users app

RUN mkdir -p app/packet_receiver && chown user:users app/packet_receiver

COPY entrypoint.sh /bin/entrypoint.sh

# ENTRYPOINT ["sh", "/entrypoint.sh"]
