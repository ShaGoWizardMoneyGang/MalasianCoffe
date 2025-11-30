package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	NODES_FILE = "sheeps.txt"
)

func ReadNodes(nodesFile string) ([]string, error) {
	data, err := os.ReadFile(nodesFile)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	nodes := []string{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			nodes = append(nodes, l)
		}
	}
	return nodes, nil
}

func main() {
	fmt.Println("HOLA MUNDO")
	nodes, err := ReadNodes(NODES_FILE)
	if err != nil {
		println("Error reading nodes:", err.Error())
	}

	for _, node := range nodes {
		semilla := time.Now().UnixNano() // Semilla aleatoria basada en el tiempo actual
		//fmt.Printf("Semilla utilizada: %d\n", semilla)
		rnd := rand.New(rand.NewSource(semilla))
		numAleatorio := rnd.Intn(100) // Número aleatorio entre 0 y 99
		fmt.Printf("Nodo: %s, número aleatorio: %d\n", node, numAleatorio)
		if numAleatorio >= 50 {
			cmd := exec.Command("sh", "-c", "docker stop -s SIGKILL "+node)
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error al detener el contenedor %s con SIGKILL: %v\n", node, err)
			} else {
				fmt.Printf("Se detuvo el container %s con SIGKILL: %s\n", node, string(output))
			}
		}
		// Sleep de 10 segundos después de cada nodo para poder observar mejor
		time.Sleep(10 * time.Second)
	}
}
