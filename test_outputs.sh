#!/bin/bash

# Colores
GREEN="\e[32m"
RED="\e[31m"
RESET="\e[0m"

# Chequear argumento
if [ $# -ne 1 ]; then
    echo "Uso: $0 FULL|RED"
    exit 1
fi

if [ "$1" == "FULL" ]; then
    EXPECTED_DIR="expected_outputs_full"
elif [ "$1" == "RED" ]; then
    EXPECTED_DIR="expected_outputs_red"
else
    echo "Argumento inválido. Usa FULL o RED."
    exit 1
fi

OUT_DIR="out"

check_file() {
    expected_file="$EXPECTED_DIR/$1"
    out_file="$OUT_DIR/$2"

    if [ ! -f "$out_file" ]; then
        echo -e "${RED}[FAIL]${RESET} No existe $out_file"
        return
    fi

    # Archivos temporales para guardar los ordenados
    temp_expected=$(mktemp)
    temp_out=$(mktemp)
    
    sort "$expected_file" > "$temp_expected"
    sort "$out_file" > "$temp_out"

    if diff -q "$temp_expected" "$temp_out" > /dev/null; then
        echo -e "${GREEN}[OK]${RESET} $2"
    else
        echo -e "${RED}[FAIL]${RESET} $2"
        echo "Diferencias (5 primeras y 5 últimas líneas):"

        diff --side-by-side --suppress-common-lines "$temp_expected" "$temp_out" | head -n 5

        echo "."
        echo "."
        echo "."

        diff --side-by-side --suppress-common-lines "$temp_expected" "$temp_out" | tail -n 5

        echo ""
    fi
    
    # Limpiar archivos temporales
    rm -f "$temp_expected" "$temp_out"
}

# Hardcodear cada test
check_file "ExpectedQuery1.csv"  "Query1.csv"
check_file "ExpectedQuery2a.csv" "Query2a.csv"
check_file "ExpectedQuery2b.csv" "Query2b.csv"
check_file "ExpectedQuery3.csv"  "Query3.csv"
check_file "ExpectedQuery4.csv"  "Query4.csv"
