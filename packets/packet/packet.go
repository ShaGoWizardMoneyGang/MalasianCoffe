package packet

import (
	"fmt"
	"strconv"
	"strings"

	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/dataset"
)

// Struct que asocia un paquete a enviar con la cola a la cual lo tiene que enviar
type OutBoundMessage struct {
	Packet Packet
	ColaSalida *middleware.MessageMiddlewareQueue
}

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


func (pu *PacketUuid) getDirID() string {
	dir_id := string(pu.uuid[0])
	return dir_id
}

type Header struct {
	// ID de la session a la que este paquete corresponde
	session_id string

	packet_uuid PacketUuid

	// TODO: esto potencialmente se puede guardar aparte en un nodo que guarde
	// las IPS. No me gusta esa decision porque introducis comunicacion extra.
	// La IP + puerto del cliente de la session
	client_ip_port string
}

func newHeader(session_id string, packet_uuid PacketUuid, client_ip_port string) Header {
	return Header{
		session_id:     session_id,
		packet_uuid:    packet_uuid,
		client_ip_port: client_ip_port,
	}
}
func (h *Header) split(id int) Header {
	new_uuid := h.packet_uuid.uuid + strconv.Itoa(id)

	new_header := Header{
		session_id: h.session_id,
		packet_uuid: PacketUuid{
			uuid: new_uuid,
			eof:  h.packet_uuid.eof,
		},
		client_ip_port: h.client_ip_port,
	}

	return new_header
}

type Packet struct {
	header Header

	payload string
}

// Mismo header, distinto payload
func ChangePayload(packet Packet, newpayload []string) []Packet {
	packets := make([]Packet, len(newpayload))

	for i, payload := range newpayload {
		newheader := packet.header
		if len(packets) > 1 {
			newheader = packet.header.split(i)
		}
		packets[i] = Packet{
			header:  newheader,
			payload: payload,
		}
	}

	return packets
}

// Devuelve packets que fueron el resultado de hacer un join
// pkt es un paquete usado solo para extraer la metadata
// datasets es el *NOMBRE* de los datasets joineados
// newPayload es el contenido
func ChangePayloadJoin(pkt Packet, datasets []string, newPayload []string) []Packet {
	datasetsIDs := make([]string, len(datasets));
	for i, dataset_name := range datasets {
		datasetID, err := dataset.DatasetToID(dataset_name)
		if err != nil {
			panic(fmt.Errorf("%s unknown dataset", dataset_name))
		}
		datasetsIDs[i] = string(datasetID)
	}

	uuid := strings.Join(datasetsIDs, "-")
	eof  := pkt.IsEOF()
	packet_uuid := PacketUuid {
		uuid: uuid,
		eof: eof,
	};

	session_id := pkt.GetSessionID()
	clientAddr := pkt.GetClientAddr()

	header     := newHeader(session_id, packet_uuid, clientAddr)
	packets := make([]Packet, len(newPayload))

	for i, payload := range newPayload {
		newheader := header
		if len(packets) > 1 {
			newheader = header.split(i)
		}
		packets[i] = Packet{
			header:  newheader,
			payload: payload,
		}
	}
	return packets
}


func (p *Packet) GetPayload() string {
	return p.payload
}

func (p *Packet) IsEOF() bool {
	return p.header.packet_uuid.eof
}

func (p *Packet) GetSessionID() string {
	return p.header.session_id
}

func (p *Packet) GetClientAddr() string {
	return p.header.client_ip_port
}

func (p *Packet) GetDirID() string {
	return p.header.packet_uuid.getDirID()
}
