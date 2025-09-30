#!/bin/bash

AUTO_ACK_IS_FALSE=$(find -name "*.go" -exec grep NO\ TOCAR\ ESTE\ BOOLEANO \{\} +  | awk '{print $2}' | sed 's/,//g')

if [ ${AUTO_ACK_IS_FALSE} != "false" ]; then
    echo "QUE DIJIMOS DEL AUTO ACK EN TRUE!!!!! >:(((((, poner en false!!!!!"
    exit 1
else
    exit 0
fi
