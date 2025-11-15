package main

import (
	"fmt"
	watchdog "malasian_coffe/system/watchdog/src"
	"net"
	"os"
	"strings"
	"time"
)

const (
	SHEEPS_FILE = "sheeps.txt"
)

func main() {
	time.Sleep(10 * time.Second) // Esperar a que los servicios estén activos
	file, err := os.ReadFile(SHEEPS_FILE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", SHEEPS_FILE, err)
	}

	services := strings.Split(string(file), "\n")
	for _, service := range services[:len(services)-1] {
		fmt.Println("Servicio:", service)
	}

	importedConstant := watchdog.HEALTHCHECK_PORT
	test_address := "joiner4_1:" + fmt.Sprint(importedConstant)
	fmt.Println("Dirección de prueba:", test_address)

	conn, err := net.Dial("udp", test_address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con %s: %v\n", test_address, err)
		return
	}
	defer conn.Close()

	fmt.Printf("Conexión UDP establecida con %s\n", test_address)
	_, err = conn.Write([]byte{0x01})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al enviar datos: %v\n", err)
		return
	}
	fmt.Println("Se envió 1 byte a través de la conexión UDP desde el Watchdog a Joiner4")

	healthcheckChannel := make(chan string)

	addr := net.UDPAddr{
		Port: watchdog.HEALTHCHECK_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	connListen, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer connListen.Close()

	buffer := make([]byte, 1024)
	for {
		_, _, err := connListen.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		fmt.Printf("Watchdog recibió PONG del Joiner 4: %s\n", string(buffer))
		healthcheckChannel <- "ping"
	}
}
