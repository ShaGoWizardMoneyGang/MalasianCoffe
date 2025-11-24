package ring

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const (
	HEALTHCHECK_PORT  int = 1958
	HEARTBEAT_PORT        = 1960
	ACK_PORT              = 1959
	HEARTBEAT_PERIOD      = 1
	HEARTBEAT_TIMEOUT     = 10
)

type WatchdogNode struct {
	ID         int
	Addr       string
	Neighbor   string // lista enlazada
	NeighborID int
	RingFile   string
}

func JoinToTheRing(id int, addr string, neighbor string, neighborID int, ringFile string) WatchdogNode {
	node :=
		WatchdogNode{
			ID:         id,
			Addr:       addr,
			Neighbor:   neighbor,
			NeighborID: neighborID,
			RingFile:   ringFile,
		}
	return node
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
			return i + 1
		}
	}
	return -1
}

// OMG LISTA CIRCULAR SISOP MEMORIES
func GetNeighbor(myID int, puppies []string) (string, int) {
	neighborID := (myID + 1) % len(puppies)
	return puppies[neighborID], neighborID
}

func (node *WatchdogNode) connectToNextNeighbor() {
	members, err := ReadRingMembers(node.RingFile)
	if err != nil {
		fmt.Printf("[%s] Error leyendo miembros del anillo: %v\n", node.Addr, err)
		return
	}
	node.NeighborID = (node.NeighborID + 1) % len(members)
	node.Neighbor = members[node.NeighborID]
	fmt.Printf("[%s] Nuevo vecino: %v\n", node.Addr, node.Neighbor)

}

func (node *WatchdogNode) WaitForACK() {
	addr := net.UDPAddr{
		Port: ACK_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	reconnectAttempts := 0
	for {
		fmt.Printf("[%s] Esperando ACK...\n", node.Addr)
		lastHeartbeat := time.Time{}
		conn.SetDeadline(time.Now().Add(HEARTBEAT_TIMEOUT * time.Second))
		//solo escucho paquetes
		n, remote, err := conn.ReadFromUDP(buffer)
		if err == nil {
			payload := string(buffer[:n])
			if payload == "ACK" {
				fmt.Printf("[%s] Recibí ACK de %s\n", node.Addr, remote.String())
				return
			}
		} else {
			fmt.Printf("[%s] Error leyendo desde UDP: %v\n", node.Addr, err)
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				if time.Since(lastHeartbeat) > HEARTBEAT_TIMEOUT*time.Second {
					fmt.Printf("[%s] No recibí mensajes de mi vecino en %d segundos. Reintento...\n", node.Addr, HEARTBEAT_TIMEOUT)
					reconnectAttempts++
					if reconnectAttempts >= 3 {
						fmt.Printf("[%s] No pude reconectar con mi vecino después de %d intentos. Asumo que cayó.\n", node.Addr, reconnectAttempts)
						node.connectToNextNeighbor()
						reconnectAttempts = 0 // resetear contador
						return
					}
				}
			} else {
				fmt.Printf("[%s] Error en ReadFromUDP: %v\n", node.Addr, err)
			}
		}
	}
}

func (node *WatchdogNode) ListenHeartbeats(forwardingChannel chan string) {
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
			// si no es ACK es el heartbeat
			fmt.Printf("[%s] Recibí: %s de %s\n", node.Addr, payload, remote.String())
			// Responde al vecino
			forwardingChannel <- payload
			prevNodeIP := strings.Split(remote.String(), ":")[0]
			prevNodeAddr := net.UDPAddr{
				Port: ACK_PORT,
				IP:   net.ParseIP(prevNodeIP),
			}
			conn.WriteToUDP([]byte("ACK"), &prevNodeAddr)
			fmt.Printf("[%s] Mandé ACK a %s\n", node.Addr, &prevNodeAddr)
		} else {
			panic(err)
		}
	}
}

func (node WatchdogNode) ForwardToNeighbor(starterID int) {
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
func (node WatchdogNode) SendHeartbeat(neighborAddr string) {
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
func (node *WatchdogNode) HeartbeatLoop() {
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
		node.SendHeartbeat(addr)
	}
}
