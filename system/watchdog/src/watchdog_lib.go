package watchdog

import (
	"fmt"
	"net"
	"os"
)

const (
	HEALTHCHECK_PORT int = 1958
)

type WatchdogListener struct {
	Conn *net.UDPConn
}

// ============================= USED BY WORKER ================================
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
	fmt.Println("Joiner 4 envió PONG al watchdog")
}

// Aca creas el SocketUDP
func CreateWatchdogListener() WatchdogListener {
	return WatchdogListener{}
}

func (wl *WatchdogListener) Listen(infoChan chan<- string) {
	addr := net.UDPAddr{
		Port: HEALTHCHECK_PORT,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	wl.Conn = conn
	defer wl.Conn.Close()

	buffer := make([]byte, 1024)
	for {
		_, addr, err := wl.Conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		fmt.Printf("Joiner 4 recibió PING del watchdog: %s\n", string(buffer))
		infoChan <- addr.String()
	}
}

// ============================= WATCHDOG LOGIC ================================
func ping() {
}
