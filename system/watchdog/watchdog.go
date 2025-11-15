package main

import (
	"fmt"
	"os"
)

const (
	SHEEPS_FILE = "sheeps.txt"
)

func main() {
	file, err := os.ReadFile(SHEEPS_FILE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", SHEEPS_FILE, err)
	}

	fmt.Print(string(file))

}
