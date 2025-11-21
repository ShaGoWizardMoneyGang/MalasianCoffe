package watchdog

import (
	"fmt"
	"net"
	"os"
	"strings"
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
		Port: 1960,
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
			fmt.Printf("[%s] Recibí: %s de %s\n", node.Addr, string(buffer[:n]), remote.String())
			// Responde al vecino
			conn.WriteToUDP([]byte("ACK"), remote)
		}
	}
}

func SendHello(node WatchdogNode) {
	neighborAddr := node.Neighbor + ":1960"
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Printf("[%s] Error enviando a %s: %v\n", node.Addr, neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("Hola soy %s", node.Addr)
	conn.Write([]byte(msg))
	fmt.Printf("[%s] Envié mensaje a %s\n", node.Addr, neighborAddr)
}
