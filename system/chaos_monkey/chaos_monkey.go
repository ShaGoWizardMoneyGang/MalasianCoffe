package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
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
	semilla_s := os.Args[1]

	semilla := time.Now().UnixNano() // Semilla aleatoria default basada en el tiempo actual
	if semilla_s != "" {
		semilla_nuevo, err := strconv.ParseInt(semilla_s, 10, 64)
		if err != nil {
			panic(err)
		}
		semilla = semilla_nuevo
	}


	// Mas chico, mas probable
	threshold_s := os.Args[2]
	threshold   := 50
	if threshold_s != "" {
		prob_minima_nuevo, err := strconv.ParseInt(threshold_s, 10, 64)
		if err != nil {
			panic(err)
		}
		threshold = int(prob_minima_nuevo)
	}

	behaviour := os.Args[3]

	nodes, err := ReadNodes(NODES_FILE)
	if err != nil {
		println("Error reading nodes:", err.Error())
	}

	fmt.Println("Moneky loco, que divertido esta.")
	println(

"            __,__ \n" +
"   .--.  .-\"     \"-.  .--. \n" +
"  / .. \\/  .-. .-.  \\/ .. \\ \n" +
" | |  '|  /   Y   \\  |'  | | \n" +
" | \\   \\  \\ 0 | 0 /  /   / | \n" +
"  \\ '- ,\\.-\"     \"-./, -' / \n" +
"   `'-' /_   ^ ^   _\\ '-'` \n" +
"       |  \\._   _./  | \n" +
"       \\   \\ `~` /   / \n" +
"jgs     '._ '-=-' _.' \n" +
"           '~---~' \n")

	fmt.Printf("Semilla:    %d \n", semilla)
	fmt.Printf("Threshold: %d \n", threshold)
	fmt.Printf("Behaviour: %s\n", behaviour)
	source  := rand.NewSource(semilla)
	rnd := rand.New(source)

	for {
		numAleatorio := rnd.Intn(100) // Número aleatorio entre 0 y 99
		nodoAleatorio := rnd.Intn(len(nodes))
		node          := nodes[nodoAleatorio]
		if numAleatorio >= threshold {
			cmd := exec.Command("sh", "-c", "docker stop -s " + behaviour + " " + node)
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error al detener el contenedor %s con %s: %v\n", node, behaviour, err)
			} else {
				fmt.Printf("Se detuvo el container %s con %s: %s\n", node, string(output), behaviour)
			}
		}
		// Sleep de 10 segundos después de cada nodo para poder observar mejor
		time.Sleep(10 * time.Second)
	}
}
