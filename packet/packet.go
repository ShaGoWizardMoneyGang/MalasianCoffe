package packet

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
	session_id uint64

	packet_uuid PacketUuid

	// TODO: esto potencialmente se puede guardar aparte en un nodo que guarde
	// las IPS. No me gusta esa decision porque introducis comunicacion extra.
	// La IP + puerto del cliente de la session
	client_ip_port string
}


func newHeader(session_id uint64, packet_uuid PacketUuid, client_ip_port string) (Header){
	return Header{
		session_id: session_id,
		packet_uuid: packet_uuid,
		client_ip_port: client_ip_port,
	}

}

type Packet struct {
	header Header

	payload string
}



