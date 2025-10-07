#!/bin/bash

GREEN="\e[32m"
RED="\e[31m"
RESET="\e[0m"

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
        ANY_FAIL=1
        return
    fi

    # Verifico cantidad de lineas
    expected_lines=$(wc -l < "$expected_file")
    out_lines=$(wc -l < "$out_file")
    
    if [ "$expected_lines" -ne "$out_lines" ]; then
        echo -e "${RED}[FAIL]${RESET} $2 - Cantidad de líneas diferente"
        echo "  Esperado: $expected_lines líneas"
        echo "  Obtenido: $out_lines líneas"
        echo ""
        ANY_FAIL=1
        return
    fi

    temp_expected=$(mktemp)
    temp_out=$(mktemp)
    
    sort "$expected_file" > "$temp_expected"
    sort "$out_file" > "$temp_out"

    if diff -q "$temp_expected" "$temp_out" > /dev/null; then
        echo -e "${GREEN}[OK]${RESET} $2 ($expected_lines líneas)"
    else
        echo -e "${RED}[FAIL]${RESET} $2 - Contenido diferente ($expected_lines líneas)"
        echo "Diferencias (5 primeras y 5 últimas líneas):"

        diff --side-by-side --suppress-common-lines "$temp_expected" "$temp_out" | head -n 5

        echo "."
        echo "."
        echo "."

        diff --side-by-side --suppress-common-lines "$temp_expected" "$temp_out" | tail -n 5

        echo ""
        ANY_FAIL=1
    fi
    
    rm -f "$temp_expected" "$temp_out"
}

check_query4() {
    expected_file="$EXPECTED_DIR/$1"
    out_file="$OUT_DIR/$2"

    if [ ! -f "$out_file" ]; then
        echo -e "${RED}[FAIL]${RESET} No existe $out_file"
        ANY_FAIL=1
        return
    fi
        if go run test_output_query4/test_output_query4.go "$expected_file" "$out_file"; then
        echo -e "${GREEN}[OK]${RESET} $2 - Todos los usuarios son correctos"
    else
        echo -e "${RED}[FAIL]${RESET} $2 - Hay usuarios inválidos"
        ANY_FAIL=1
    fi
}

check_file "ExpectedQuery1.csv"  "Query1.csv"
check_file "ExpectedQuery2a.csv" "Query2a.csv"
check_file "ExpectedQuery2b.csv" "Query2b.csv"
check_file "ExpectedQuery3.csv"  "Query3.csv"
check_query4 "ExpectedQuery4.csv"  "Query4.csv"

# If any check failed, exit with -1
if [ "${ANY_FAIL:-0}" -ne 0 ]; then
    echo -e "${RED}Some tests failed.${RESET}"
    exit -1
fi
