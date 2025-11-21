package watchdog

import (
	"fmt"
	"net"
	"os"
	"time"
)

const (
	HEALTHCHECK_PORT  int = 1958
	HEARTBEAT_PORT        = 1959
	HEARTBEAT_PERIOD      = 3
	HEARTBEAT_TIMEOUT     = 7
)

type WatchdogListener struct {
	conn net.UDPConn
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
func SendHeartbeat(replicaAddr string) {
	conn, err := net.Dial("udp", replicaAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con replica %s: %v\n", replicaAddr, err)
		return
	}
	defer conn.Close()
	conn.Write([]byte("HEARTBEAT"))
}

// Envía heartbeats periódicos a todas las réplicas
func HeartbeatLoop(replicaList []string) {
	// LOCURA LO QUE ES ESTE TICKER
	// El ticker es un objeto que genera eventos cada cierto intervalo
	ticker := time.NewTicker(HEARTBEAT_PERIOD * time.Second)
	defer ticker.Stop()
	// este bucle espera a que pase el intervalo del ticker
	// cada vez que pase el tiempo, el canal ticker.c recibe un valor y el codigo dentro del bucle se ejecuta
	for {
		<-ticker.C
		for _, replica := range replicaList {
			addr := replica + ":" + fmt.Sprint(HEARTBEAT_PORT)
			SendHeartbeat(addr)
		}
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

// ============================= USED BY WORKER ================================
func CreateWatchdogListener() WatchdogListener {
	addr := net.UDPAddr{
		Port: HEALTHCHECK_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	return WatchdogListener{conn: *conn}
}

func (wl *WatchdogListener) Pong(responseIP string) {
	responseAddress := responseIP + ":" + fmt.Sprint(HEALTHCHECK_PORT)
	fmt.Println(responseAddress)
	conn, err := net.Dial("udp", responseAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con %s: %v\n", responseAddress, err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte{0x01})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al enviar datos: %v\n", err)
		return
	}
	fmt.Printf("PONG recibido de %s\n", responseAddress)
}

func (wl *WatchdogListener) Listen(infoChan chan<- string) {
	defer wl.conn.Close()
	buffer := make([]byte, 1024)
	for {
		_, addr, err := wl.conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		fmt.Printf("Joiner recibió PING del watchdog: %s\n", string(buffer))
		infoChan <- addr.String()
	}
}

// ============================= WATCHDOG LOGIC ================================
func Ping(address string) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al conectar con %s: %v\n", address, err)
		return err
	}
	defer conn.Close()

	fmt.Printf("Conexión UDP establecida con %s\n", address)
	_, err = conn.Write([]byte{0x01})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al enviar datos: %v\n", err)
		return err
	}
	return nil
}
