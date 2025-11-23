package main

import (
	"fmt"
	"malasian_coffe/utils/ring"
	"os"
	"os/exec"
	"time"
)

const (
	SHEEPS_FILE       = "sheeps.txt"
	PUPPIES_FILE      = "puppies.txt"
	RING_MEMBERS_FILE = "ring.txt"
	MAX_RETRIES       = 3
	TIMEOUT           = 2
)

func restartContainer(name string) {
	cmd := exec.Command("sh", "-c", "docker stop "+name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al detener el contenedor %s: %v\n", name, err)
	} else {
		fmt.Printf("Se detuvo el container %s: %s\n", name, string(output))
		time.Sleep(2 * time.Second)
		// DEJO DOCKER RESTART
		cmd = exec.Command("sh", "-c", "docker restart "+name)
		output, err = cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error al iniciar el contenedor %s: %v\n", name, err)
		} else {
			fmt.Printf("Se inició el contenedor %s: %s\n", name, string(output))
		}
	}
}

func main() {
	fmt.Println("Esperando a que arranque el sistema...")
	time.Sleep(5 * time.Second)

	// PRUEBA DE ANILLO
	myName, err := os.Hostname() //watchdog_1, watchdog_2, ...
	if err != nil {
		panic("No se pudo obtener el hostname")
	}
	members, err := ring.ReadRingMembers(RING_MEMBERS_FILE)
	if err != nil {
		panic(err)
	}
	myID := ring.FindID(myName, members)
	neighbor := ring.GetNeighbor(myID, members)
	fmt.Printf("Soy %s, mi vecino en el anillo es %s\n", myName, neighbor)

	ringNode := ring.WatchdogNode{ID: myID, Addr: myName, Neighbor: neighbor}

	go ring.ListenRing(ringNode)
	amIStarter := os.Args[1]
	if amIStarter != "STARTER" {
		select {}
	}

	ring.HeartbeatLoop(ringNode, members)

	fmt.Println("Soy el líder, comienzo watchdog")
	// file, err := os.ReadFile(SHEEPS_FILE)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", SHEEPS_FILE, err)
	// }

	// listOfServices := strings.Split(string(file), "\n")
	// services := make([]string, 0, len(listOfServices))
	// for _, s := range listOfServices {
	// 	s = strings.TrimSpace(s)
	// 	if s != "" {
	// 		services = append(services, s)
	// 		fmt.Println("Servicio:", s)
	// 	}
	// }

	// // Lista de réplicas harcodeada con 2
	// // TODO: parametrizar
	// // replicaList := []string{"watchdog_2", "watchdog_3"}

	// file, err = os.ReadFile(PUPPIES_FILE)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", PUPPIES_FILE, err)
	// }

	// listOfPuppies := strings.Split(string(file), "\n")
	// members = make([]string, 0, len(listOfPuppies))
	// for _, p := range listOfPuppies {
	// 	p = strings.TrimSpace(p)
	// 	if p != "" {
	// 		members = append(members, p)
	// 		fmt.Println("Réplicas:", p)
	// 	}
	// }

	// fmt.Println(members)

	// addr := net.UDPAddr{
	// 	Port: ring.HEALTHCHECK_PORT,
	// 	IP:   net.ParseIP("0.0.0.0"),
	// }
	// connListen, err := net.ListenUDP("udp", &addr)
	// if err != nil {
	// 	panic(err)
	// }
	// defer connListen.Close()

	// buffer := make([]byte, 1024)
	// for {
	// 	for _, serviceName := range services {
	// 		healthCheckAddress := serviceName + ":" + fmt.Sprint(watchdog.HEALTHCHECK_PORT)
	// 		successfulHealthcheck := false
	// 		for attempt := 1; attempt <= MAX_RETRIES; attempt++ {
	// 			fmt.Printf("Intento %d de %d: enviando PING a %s\n", attempt, MAX_RETRIES, healthCheckAddress)
	// 			// Mando PING
	// 			err := watchdog.Ping(healthCheckAddress)
	// 			if err != nil {
	// 				fmt.Fprintf(os.Stderr, "Error enviando ping a %s: %v\n", healthCheckAddress, err)
	// 			} else {
	// 				// Acá con SetReadDeadline defino que después de la hora actual + 2 segundos vence el timeout
	// 				connListen.SetReadDeadline(time.Now().Add(TIMEOUT * time.Second))
	// 				n, _, err := connListen.ReadFromUDP(buffer)
	// 				// Si el Read NO devuelve error, entonces se recibió el PONG y salimos
	// 				if err == nil {
	// 					fmt.Printf("Watchdog recibió PONG del %s: %s\n", serviceName, string(buffer[:n]))
	// 					successfulHealthcheck = true
	// 					break
	// 				}
	// 				// Si el error es de network y es un timeout, el PONG No llegó en el tiempo establecido (2seg)
	// 				netError, isNetError := err.(net.Error)
	// 				if isNetError == true && netError.Timeout() {
	// 					fmt.Printf("No se recibió PONG en %v segundos\n ", TIMEOUT)
	// 				}
	// 			}
	// 			// Espero un poco antes del siguiente intento
	// 			time.Sleep(1 * time.Second)
	// 		}

	// 		if successfulHealthcheck == false {
	// 			fmt.Printf("No se recibió PONG tras %d intentos. Reiniciando %s\n", MAX_RETRIES, serviceName)
	// 			restartContainer(serviceName)
	// 		} else {
	// 			fmt.Println("El servicio respondió correctamente, no hace falta reiniciar")
	// 		}
	// 	}
	// 	time.Sleep(watchdog.HEARTBEAT_PERIOD * time.Second)
	// }
}

/*
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
