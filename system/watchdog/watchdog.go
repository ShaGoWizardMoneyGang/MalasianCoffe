package main

import (
	"fmt"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/ring"
	"net"
	"os"
	"os/exec"
	"strings"
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
	time.Sleep(10 * time.Second)

	// PRUEBA DE ANILLO
	myName, err := os.Hostname()
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

	go ring.ListenRing(ring.WatchdogNode{ID: myID, Addr: myName, Neighbor: neighbor})
	time.Sleep(2 * time.Second)
	ring.SendHello(ring.WatchdogNode{ID: myID, Addr: myName, Neighbor: neighbor})
	// PARA PRUEBA LOS SLEEPS E
	time.Sleep(10 * time.Second)

	amILeader := os.Args[1]
	if amILeader != "LEADER" {
		// Es réplica
		fmt.Println("Hola soy una réplica, escucho heartbeats del líder")
		watchdog.ReplicaHeartbeatLoop() // loop para escuchar heartbeats
		return
	}
	fmt.Println("Soy el líder, comienzo watchdog")
	file, err := os.ReadFile(SHEEPS_FILE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", SHEEPS_FILE, err)
	}

	listOfServices := strings.Split(string(file), "\n")
	services := make([]string, 0, len(listOfServices))
	for _, s := range listOfServices {
		s = strings.TrimSpace(s)
		if s != "" {
			services = append(services, s)
			fmt.Println("Servicio:", s)
		}
	}

	// Lista de réplicas harcodeada con 2
	// TODO: parametrizar
	// replicaList := []string{"watchdog_2", "watchdog_3"}

	file, err = os.ReadFile(PUPPIES_FILE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "No se pudo abrir el archivo %s: %v\n", PUPPIES_FILE, err)
	}

	listOfPuppies := strings.Split(string(file), "\n")
	members = make([]string, 0, len(listOfPuppies))
	for _, p := range listOfPuppies {
		p = strings.TrimSpace(p)
		if p != "" {
			members = append(members, p)
			fmt.Println("Réplicas:", p)
		}
	}

	fmt.Println(members)

	go watchdog.HeartbeatLoop(members)

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
		for _, serviceName := range services {
			healthCheckAddress := serviceName + ":" + fmt.Sprint(watchdog.HEALTHCHECK_PORT)
			successfulHealthcheck := false
			for attempt := 1; attempt <= MAX_RETRIES; attempt++ {
				fmt.Printf("Intento %d de %d: enviando PING a %s\n", attempt, MAX_RETRIES, healthCheckAddress)
				// Mando PING
				err := watchdog.Ping(healthCheckAddress)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error enviando ping a %s: %v\n", healthCheckAddress, err)
				} else {
					// Acá con SetReadDeadline defino que después de la hora actual + 2 segundos vence el timeout
					connListen.SetReadDeadline(time.Now().Add(TIMEOUT * time.Second))
					n, _, err := connListen.ReadFromUDP(buffer)
					// Si el Read NO devuelve error, entonces se recibió el PONG y salimos
					if err == nil {
						fmt.Printf("Watchdog recibió PONG del %s: %s\n", serviceName, string(buffer[:n]))
						successfulHealthcheck = true
						break
					}
					// Si el error es de network y es un timeout, el PONG No llegó en el tiempo establecido (2seg)
					netError, isNetError := err.(net.Error)
					if isNetError == true && netError.Timeout() {
						fmt.Printf("No se recibió PONG en %v segundos\n ", TIMEOUT)
					}
				}
				// Espero un poco antes del siguiente intento
				time.Sleep(1 * time.Second)
			}

			if successfulHealthcheck == false {
				fmt.Printf("No se recibió PONG tras %d intentos. Reiniciando %s\n", MAX_RETRIES, serviceName)
				restartContainer(serviceName)
			} else {
				fmt.Println("El servicio respondió correctamente, no hace falta reiniciar")
			}
		}
		time.Sleep(watchdog.HEARTBEAT_PERIOD * time.Second)
	}
}
