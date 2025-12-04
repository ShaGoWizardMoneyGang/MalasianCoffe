#!/bin/sh

# Este script de mierda lo hago porque docker es una cagada.
# Por algun motivo, los permisos de un volume defaultean a root INCLUSO SI el
# directorio YA EXISTE.
# Este comportamiento es no solo malvado, sino que tambien maquiavelico.
# Me causo mucha tristeza y sufrimiento.

# Docker, te odio.
# - Tu odiador secreto

umask 0666
mkdir -p /app/packet_receiver/tmp

chown -R user:users /app
chown -R user:users /app/packet_receiver
chown -R user:users /app/packet_receiver/tmp
chmod -R 777 /app
chmod -R 777 /app/packet_receiver
chmod -R 777 /app/packet_receiver/tmp
