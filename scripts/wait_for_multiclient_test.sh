#!/bin/bash

cantClientes=$(grep cliente compose.config | awk -F = '{print $2}')

clientes_string=""
for (( i=1; i<=$cantClientes; i++ )); do
    ( 
        echo "Waiting client${i}..."
        docker wait "client${i}" > /dev/null
        echo "✅ Client ${i} finished!"
    )&
    clientes_string+="client${i} "
done

wait