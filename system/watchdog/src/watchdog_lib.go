package watchdog

const (
	HEALTHCHECK_PORT int = 1958
)

type WatchdogListener struct {
	// Socket UDP
	// net.UDPConn
}

// ============================= USED BY WORKER ================================
func pong() {
}

// Aca creas el SocketUDP
func CreateWatchdogListener() WatchdogListener {
	return WatchdogListener{}
}

func (wl *WatchdogListener) Listen() {

}

// ============================= WATCHDOG LOGIC ================================
func ping() {
}
