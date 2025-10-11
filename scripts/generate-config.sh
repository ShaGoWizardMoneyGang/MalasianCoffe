#!/bin/bash

config=$1

estados() {
    echo "Las opciones disponibles son:"
    echo "- '1DeCada': 1 worker de cada tipo"
    echo "- '1DeCadaMultiplesClientes: 1 worker de cada tipo, multiples clientes'"
    echo "- 'MuchosSinEstado': Varios workers de los que no tienen estado"
}

if [ -z ${config} ]; then
    echo "No se detecto ningun config deseado"
    estados
elif [ ${config} = "1DeCada" ]; then
    cp -f .github/data/compose1DeCada.config compose.config
elif [ ${config} = "MuchosSinEstado" ]; then
    # Viva la anarquia ✊
    # https://es.wikipedia.org/wiki/Anarqu%C3%ADa
    cp -f .github/data/composeMuchosSinEstado.config compose.config
elif [ ${config} = "1DeCadaMultiplesClientes" ]; then
    cp -f .github/data/compose1DeCadaMultiplesClientes.config compose.config
else
    echo "Opcion '${config}' no reconocida"
    estados
    exit 1
fi
