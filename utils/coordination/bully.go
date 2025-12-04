package bully

import (
	"fmt"
	"malasian_coffe/utils/network"
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
	HEARTBEAT_PORT    = 1960
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
			node.sendHeartbeatToMember(member)
		}
	}
}

func (node *WatchdogNode) sendHeartbeatToMember(member string) {
	if member == node.Addr {
		return
	}
	neighborAddr := net.JoinHostPort(member, strconv.Itoa(HEARTBEAT_PORT))
	conn, err := net.Dial("udp", neighborAddr)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("%d,%s", node.ID, node.Addr)
	conn.Write([]byte(msg))
}

func (node *WatchdogNode) ListenHeartbeats() {
	conn, err := network.CreateUDPListener(HEARTBEAT_PORT)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		if node.AmIMaster() {
			conn.Close()
			return
		}
		conn.SetReadDeadline(time.Now().Add(HEARTBEAT_TIMEOUT * time.Second))
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if os.IsTimeout(err) && !node.Coordinating {
				fmt.Fprintf(os.Stderr, "Error: No se recibió nada en 3 segundos. Asumo que el lider murió, arranca elección.\n")
			} else {
				fmt.Fprintf(os.Stderr, "Error al recibir heartbeat: %v\n. Arranca eleccion.", err)
			}
			node.startNewElection()
			continue
		}
		payload := string(buffer[:n])
		switch {
		case strings.HasPrefix(payload, "ELECTION"):
			node.handleElectionMessage(payload, remote.IP.String())
		case payload == "OK":
		case strings.HasPrefix(payload, "COORDINATOR"):
			node.handleCoordinatorMessage(payload)
		default:
		}
	}
}

func (node *WatchdogNode) startNewElection() {
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
		node.sendCoordinationMessage()
	}
}

func (node *WatchdogNode) sendCoordinationMessage() {
	fmt.Printf("[%s] No se encontraron miembros con ID más alto, soy el nuevo líder\n", node.Addr)
	for _, member := range node.Nodes {
		if member == node.Addr {
			continue
		}
		neighborAddr := net.JoinHostPort(member, strconv.Itoa(HEARTBEAT_PORT))
		conn, err := net.Dial("udp", neighborAddr)
		if err != nil {
			// fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
			continue
		}
		defer conn.Close()
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
		// fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return false
	}
	defer conn.Close()
	msg := fmt.Sprintf("ELECTION[%d]", node.ID)
	conn.Write([]byte(msg))
	return true
}

func (node *WatchdogNode) handleElectionMessage(payload string, senderAddr string) {
	senderIDStr := strings.TrimSuffix(strings.TrimPrefix(payload, "ELECTION["), "]")
	senderID, err := strconv.Atoi(senderIDStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al parsear senderID del mensaje de elección: %v\n", err)
		return
	}
	if senderID < node.ID {
		node.sendOKMessage(senderAddr)
	}
}

func (node *WatchdogNode) handleCoordinatorMessage(payload string) {
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
		// fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", neighborAddr, err)
		return
	}
	defer conn.Close()
	msg := "OK"
	conn.Write([]byte(msg))
}
