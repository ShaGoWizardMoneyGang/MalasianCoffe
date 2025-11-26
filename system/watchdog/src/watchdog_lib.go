package watchdog

import (
	"fmt"
	"net"
	"os"
)

const (
	HEALTHCHECK_PORT  int = 1958
	HEARTBEAT_PORT        = 1959
	HEARTBEAT_PERIOD      = 3
	HEARTBEAT_TIMEOUT     = 7
)

type WatchdogListener struct {
	Conn        *net.UDPConn
	KeepRunning bool
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
	return WatchdogListener{Conn: conn, KeepRunning: true}
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
	fmt.Print("-----------------------------------HOLAAAAA ENTRE PAPI")

	//defer wl.conn.Close()
	buffer := make([]byte, 1024)
	fmt.Println("-----------------------------------HOLA ESTOY ANTES DEL FOR DEL LISTEN")
	for {
		fmt.Println("-----------------------------------HOLA ESTOY DESPUES DEL FOR DEL LISTEN")
		fmt.Printf("------------------------------------KeepRunning value: %v\n", wl.KeepRunning)

		if !wl.KeepRunning {
			fmt.Println("@@@@ WatchdogListener: cerrando la conexión UDP")
			//wl.Conn.Close()
			return
		}
		_, addr, err := wl.Conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}
		fmt.Printf("Réplica recibió PING del watchdog: %s\n", string(buffer))
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
