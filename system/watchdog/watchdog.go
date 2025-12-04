package main

import (
	"fmt"
	watchdog "malasian_coffe/system/watchdog/src"
	bully "malasian_coffe/utils/coordination"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	SHEEPS_FILE          = "sheeps.txt"
	MEMBERS_FILE         = "members.txt"
	MAX_RETRIES          = 3
	TIMEOUT              = 2
	HEALTHCHECK_PORT int = 1958
)

func restartContainer(name string) {
	cmd := exec.Command("sh", "-c", "docker stop "+name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al detener el contenedor %s: %v\n", name, err)
	} else {
		fmt.Printf("Se detuvo el container %s: %s\n", name, string(output))
		time.Sleep(2 * time.Second)
		cmd = exec.Command("sh", "-c", "docker restart "+name)
		output, err = cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error al iniciar el contenedor %s: %v\n", name, err)
		} else {
			fmt.Printf("Se inició el contenedor %s: %s\n", name, string(output))
		}
	}
}

func watchSheeps() {
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

	addr := net.UDPAddr{
		Port: HEALTHCHECK_PORT,
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
				err := watchdog.Ping(healthCheckAddress)
				if err != nil {
					// fmt.Fprintf(os.Stderr, "Error enviando ping a %s: %v\n", healthCheckAddress, err)
				} else {
					connListen.SetReadDeadline(time.Now().Add(TIMEOUT * time.Second))
					_, _, err := connListen.ReadFromUDP(buffer)
					if err == nil {
						// fmt.Printf("Watchdog recibió PONG del %s: %s\n", serviceName, string(buffer[:n]))
						successfulHealthcheck = true
						break
					}
				}
				// Espero un poco antes del siguiente intento
				time.Sleep(1 * time.Second)
			}

			if !successfulHealthcheck {
				fmt.Printf("No se recibió PONG tras %d intentos. Reiniciando %s\n", MAX_RETRIES, serviceName)
				restartContainer(serviceName)
			}
		}
		time.Sleep(watchdog.HEARTBEAT_PERIOD * time.Second)
	}
}

func ReadMembers(membersFile string) ([]string, error) {
	data, err := os.ReadFile(membersFile)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	members := []string{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			members = append(members, l)
		}
	}
	return members, nil
}

func GetID(myName string, members []string) int {
	for i, name := range members {
		if name == myName {
			return i + 1
		}
	}
	return -1
}

func main() {
	time.Sleep(5 * time.Second)

	myName, err := os.Hostname()
	if err != nil {
		panic("No se pudo obtener el hostname")
	}
	members, err := ReadMembers(MEMBERS_FILE)
	if err != nil {
		panic(err)
	}
	myID := GetID(myName, members)

	node := bully.WatchdogNode{
		ID:           myID,
		Addr:         myName,
		MasterID:     -1,
		Nodes:        members,
		Coordinating: false,
	}

	for {
		if node.AmIMaster() {
			fmt.Println("Soy el iniciador, comienzo el bucle de latidos")
			go watchSheeps()
			node.MasterID = node.ID
			node.BroadcastHeartbeat()
		} else {
			watchdogListener := watchdog.CreateWatchdogListener()
			healthcheckChannel := make(chan string)
			go watchdogListener.Listen(healthcheckChannel)
			go func() {
				for serviceName := range healthcheckChannel {
					IP := strings.Split(serviceName, ":")[0]
					watchdogListener.Pong(IP)
				}
			}()
			node.ListenHeartbeats()
			watchdogListener.KeepRunning = false
			watchdogListener.Conn.Close()
		}
	}
}
