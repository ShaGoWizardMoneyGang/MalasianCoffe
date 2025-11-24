package bully

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type WatchdogNode struct {
	ID           int
	Addr         string
	MasterID     int
	Nodes        []string
	Coordinating bool
}

const (
	HEALTHCHECK_PORT int = 1958
	HEARTBEAT_PORT       = 1960
	// COORD_PORT            = 1959
	HEARTBEAT_PERIOD  = 1
	HEARTBEAT_TIMEOUT = 3
)

func (node *WatchdogNode) AmIMaster() bool {
	return node.ID == node.MasterID
}

func (node *WatchdogNode) BroadcastHeartbeat() {
	ticker := time.NewTicker(HEARTBEAT_PERIOD * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, member := range node.Nodes {
			node.sendHearbeatToMember(member)
		}
	}
}

func (node *WatchdogNode) sendHearbeatToMember(member string) {
	if member == node.Addr {
		return //no me envío a mí mismo
	}
	neighborAddr := net.JoinHostPort(member, strconv.Itoa(HEARTBEAT_PORT))
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return
	}
	defer conn.Close()
	fmt.Printf("[%s] Sending heartbeat to %s\n", node.Addr, neighborAddr)
	msg := fmt.Sprintf("%d,%s", node.ID, node.Addr)
	conn.Write([]byte(msg))
}

func (node *WatchdogNode) ListenHeartbeats() {
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
		if node.AmIMaster() {
			fmt.Printf("[%s] Soy el líder, no escucho latidos\n", node.Addr)
			return
		}
		conn.SetReadDeadline(time.Now().Add(HEARTBEAT_TIMEOUT * time.Second))
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			//TODO revisar que os.IsTimeout esté bien usado acá
			if os.IsTimeout(err) && !node.Coordinating { //si no estan coordinando, lo tiro
				fmt.Fprintf(os.Stderr, "Error: nothing received in 3 seconds, assuming leader died\n")
				node.StartNewElection()
			} else {
				fmt.Fprintf(os.Stderr, "Error al recibir heartbeat: %v\n", err)
			}
			continue
		}
		payload := string(buffer[:n])
		switch {
		case strings.HasPrefix(payload, "ELECTION"):
			fmt.Printf("[%s] Election message received: %s\n", node.Addr, payload)
			node.HandleElectionMessage(payload, remote.IP.String())
		case payload == "OK":
			fmt.Printf("[%s] Recibí mensaje OK, sé que hay un líder más alto\n", node.Addr)
		case strings.HasPrefix(payload, "COORDINATOR"):
			fmt.Printf("[%s] Coordinator message received: %s\n", node.Addr, payload)
			node.HandleCoordinatorMessage(payload)
		default:
			fmt.Printf("[%s] Heartbeat received: %s\n", node.Addr, payload)
		}
	}
}

func (node *WatchdogNode) StartNewElection() {
	node.Coordinating = true
	fmt.Printf("[%s] Starting new election...\n", node.Addr)
	higherIDFound := false
	for _, member := range node.Nodes {
		memberID, err := strconv.Atoi(strings.Split(member, "_")[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error al parsear ID de miembro %s: %v\n", member, err)
			continue
		}
		if memberID > node.ID {
			fmt.Printf("[%s] Miembro con ID más alto %s, mando elección\n", node.Addr, member)
			reachedNode := node.sendElectionMessage(member)
			if reachedNode {
				higherIDFound = true
			}
		}
	}
	if !higherIDFound {
		node.SendCoordinationMessage()
	}
}

func (node *WatchdogNode) SendCoordinationMessage() {
	fmt.Printf("[%s] No se encontraron miembros con ID más alto, soy el nuevo líder\n", node.Addr)
	for _, member := range node.Nodes {
		if member == node.Addr {
			continue
		}
		neighborAddr := net.JoinHostPort(member, strconv.Itoa(HEARTBEAT_PORT))
		conn, err := net.Dial("udp", neighborAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
			continue
		}
		defer conn.Close()
		fmt.Printf("[%s] Sending coordinator message to %s\n", node.Addr, neighborAddr)
		msg := fmt.Sprintf("COORDINATOR[%d]", node.ID)
		conn.Write([]byte(msg))
	}
	node.MasterID = node.ID
	node.Coordinating = false
}

func (node *WatchdogNode) sendElectionMessage(member string) bool {
	neighborAddr := net.JoinHostPort(member, strconv.Itoa(HEARTBEAT_PORT))
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return false
	}
	defer conn.Close()
	fmt.Printf("[%s] Sending election message to %s\n", node.Addr, neighborAddr)
	msg := fmt.Sprintf("ELECTION[%d]", node.ID)
	conn.Write([]byte(msg))
	return true
}

func (node *WatchdogNode) HandleElectionMessage(payload string, senderAddr string) {
	senderIDStr := strings.TrimSuffix(strings.TrimPrefix(payload, "ELECTION["), "]")
	senderID, err := strconv.Atoi(senderIDStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al parsear senderID del mensaje de elección: %v\n", err)
		return
	}
	if senderID < node.ID {
		fmt.Printf("[%s] Recibí mensaje de elección de %d, respondo con OK\n", node.Addr, senderID)
		node.sendOKMessage(senderAddr)
	}
}

func (node *WatchdogNode) HandleCoordinatorMessage(payload string) {
	newMasterIDStr := strings.TrimSuffix(strings.TrimPrefix(payload, "COORDINATOR["), "]")
	newMasterID, err := strconv.Atoi(newMasterIDStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al parsear newMasterID del mensaje de coordinador: %v\n", err)
		return
	}
	node.MasterID = newMasterID
	fmt.Printf("[%s] Nuevo líder es %d\n", node.Addr, newMasterID)
	node.Coordinating = false
}

func (node *WatchdogNode) sendOKMessage(memberAddr string) {
	neighborAddr := net.JoinHostPort(memberAddr, strconv.Itoa(HEARTBEAT_PORT))
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return
	}
	defer conn.Close()
	fmt.Printf("[%s] Sending OK message to %s\n", node.Addr, neighborAddr)
	msg := "OK"
	conn.Write([]byte(msg))
}
