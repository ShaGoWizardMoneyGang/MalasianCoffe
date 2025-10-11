#!/bin/bash

cantClientes=$(grep cliente compose.config | awk -F = '{print $2}')

clientes_string=""
for (( i=1; i<=$cantClientes; i++ )); do
    clientes_string+="client${i} "
done

comando="time docker wait ${clientes_string}"


${comando}
