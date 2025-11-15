package watchdog

import (
	"fmt"
	"net"
)

const (
	HEALTHCHECK_PORT int = 1958
)

type WatchdogListener struct {
	Conn *net.UDPConn
}

// ============================= USED BY WORKER ================================
func pong() {
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
		_, _, err := wl.Conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		fmt.Printf("Received: %s\n", string(buffer))
		infoChan <- "ping"
	}
}

// ============================= WATCHDOG LOGIC ================================
func ping() {
}
