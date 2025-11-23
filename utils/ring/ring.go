package ring

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	HEALTHCHECK_PORT  int = 1958
	HEARTBEAT_PORT        = 1960
	HEARTBEAT_PERIOD      = 3
	HEARTBEAT_TIMEOUT     = 7
)

type WatchdogNode struct {
	ID       int
	Addr     string
	Neighbor string // lista enlazada
}

func ReadRingMembers(ringFile string) ([]string, error) {
	data, err := os.ReadFile(ringFile)
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

func FindID(myName string, puppies []string) int {
	for i, name := range puppies {
		if name == myName {
			return i
		}
	}
	return -1
}

// OMG LISTA CIRCULAR SISOP MEMORIES
func GetNeighbor(myID int, puppies []string) string {
	return puppies[(myID+1)%len(puppies)]
}

func ListenRing(node WatchdogNode) {
	addr := net.UDPAddr{
		Port: HEARTBEAT_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, remote, err := conn.ReadFromUDP(buffer)
		if err == nil {
			payload := string(buffer[:n])
			switch payload {
			case "ACK":
				fmt.Printf("[%s] Recibí ACK de %s\n", node.Addr, remote.String())
			default:
				//si no es ACK es el heartbeat
				fmt.Printf("[%s] Recibí: %s de %s\n", node.Addr, string(buffer[:n]), remote.String())
				// Responde al vecino
				prevNodeIP := strings.Split(remote.String(), ":")[0]
				prevNodeAddr := net.UDPAddr{
					Port: HEALTHCHECK_PORT,
					IP:   net.ParseIP(prevNodeIP),
				}
				conn.WriteToUDP([]byte("ACK"), &prevNodeAddr)
				fmt.Printf("[%s] Mandé ACK a %s\n", node.Addr, &prevNodeAddr)

				fmt.Printf("[%s] Forwardeo a mi vecino %s\n", node.Addr, node.Neighbor)
				starterID, _ := strconv.Atoi(strings.Split(payload, ",")[0])
				if starterID == node.ID {
					fmt.Printf("[%s] El mensaje volvió a mí, no lo reenvío\n", node.Addr)
					continue
				}
				forwardToNeighbor(node, starterID)
			}
		}
	}
}

func forwardToNeighbor(node WatchdogNode, starterID int) {
	neighborAddr := node.Neighbor + ":" + fmt.Sprint(HEARTBEAT_PORT)
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Printf("[%s] Error enviando a %s: %v\n", node.Addr, neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("%d,%s", starterID, node.Addr)
	conn.Write([]byte(msg))
	fmt.Printf("[%s] Envié mensaje a %s\n", node.Addr, neighborAddr)
}

func SendHello(node WatchdogNode) {
	neighborAddr := node.Neighbor + ":" + fmt.Sprint(HEARTBEAT_PORT)
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Printf("[%s] Error enviando a %s: %v\n", node.Addr, neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("%d,%s", node.ID, node.Addr)
	conn.Write([]byte(msg))
	fmt.Printf("[%s] Envié mensaje a %s\n", node.Addr, neighborAddr)
}

// Lógica de comunicación Master-Réplicas
/*
Si el watchdog es réplica:
- Espera los heartbeats del lider, si no llegan avisa
Si el watchdog es el líder:
- Lee el archivo de sheeps.txt, guarda los servicios, etc
- Define lista de réplicas (a parametrizar)
- Arranca una go routine que periódicamente envia por UDP "Heartbeat" a cada réplica
usando HeartbeatLoop()

Después todo sigue practicamente igual a lo que ya teníamos
*/

// ============================= HEARTBEAT ================================

// Envía heartbeat
func SendHeartbeat(node WatchdogNode, neighborAddr string) {
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("%d,%s", node.ID, node.Addr)
	conn.Write([]byte(msg))
}

// Envía heartbeats periódicos a su vecino que va a propagar el mensaje
func HeartbeatLoop(node WatchdogNode, replicaList []string) {
	// LOCURA LO QUE ES ESTE TICKER
	// El ticker es un objeto que genera eventos cada cierto intervalo
	ticker := time.NewTicker(HEARTBEAT_PERIOD * time.Second)
	defer ticker.Stop()
	// este bucle espera a que pase el intervalo del ticker
	// cada vez que pase el tiempo, el canal ticker.c recibe un valor y el codigo dentro del bucle se ejecuta
	for {
		<-ticker.C
		addr := node.Neighbor + ":" + fmt.Sprint(HEARTBEAT_PORT)
		fmt.Printf("Enviando heartbeat a %s\n", addr)
		SendHeartbeat(node, addr)
	}
}

// ============================= REPLICA ================================

// Espera los heartbeats del lider, si no llegan avisa
func ReplicaHeartbeatLoop() {
	addr := net.UDPAddr{
		Port: HEARTBEAT_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	// Abro socket
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	// Guardo cuándo llegó el último heartbeat
	lastHeartbeat := time.Now()
	for {
		// Timeout para la lectura, si nada llega en HEARTBEAT_TIMEOUT falla
		conn.SetReadDeadline(time.Now().Add(HEARTBEAT_TIMEOUT * time.Second))
		n, _, err := conn.ReadFromUDP(buffer)
		// si recibi mensjae y es "HEARTBEAT", actualizo el tiempo del último heartbeat
		if err == nil && string(buffer[:n]) == "HEARTBEAT" {
			lastHeartbeat = time.Now()
			fmt.Println("Heartbeat recibido del líder")
			// si no recibe nada en ese tiempo, avisa que no llegó
		} else if time.Since(lastHeartbeat) > HEARTBEAT_TIMEOUT*time.Second {
			fmt.Println("No recibí heartbeat del líder en el tiempo esperado")
		}
	}
}
