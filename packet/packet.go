package packet

import (
)

// Formato:
// String del estilo A.B.C.D...
// Donde:
// - A: A es el identificador del archivo del cual se obtuvo este paquete
// - El resto de los campos representan la particion del archivo, es decir cuantas particiones y subparticiones tuvo el archivo.
type PacketUuid struct {
	uuid string

	// End of file. Este paquete es el ultimo paquete del archivo correspondiente
	eof bool
}

type Header struct {
	// ID de la session a la que este paquete corresponde
	session_id uint32

	packet_uuid PacketUuid

	// La IP + puerto del cliente de la session
	client_ip_port string
}

type Packet struct {
	header Header

	payload []byte
}
