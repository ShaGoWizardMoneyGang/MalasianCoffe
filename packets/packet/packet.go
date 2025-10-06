package packet

import (
	"fmt"
	"math"
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
type packetUuid struct {
	uuid string

	// End of file. Este paquete es el ultimo paquete del archivo correspondiente
	eof bool
}


func (pu *packetUuid) getDirID() string {
	dir_id := string(pu.uuid[0])
	return dir_id
}

func newPacketUuid(uuid string, eof bool) packetUuid {
	return packetUuid{
		uuid: uuid,
		eof: eof,
	}
}

type Header struct {
	// ID de la session a la que este paquete corresponde
	session_id string

	packet_uuid packetUuid

	// TODO: esto potencialmente se puede guardar aparte en un nodo que guarde
	// las IPS. No me gusta esa decision porque introducis comunicacion extra.
	// La IP + puerto del cliente de la session
	client_ip_port string
}

func newHeader(session_id string, packet_uuid packetUuid, client_ip_port string) Header {
	return Header{
		session_id:     session_id,
		packet_uuid:    packet_uuid,
		client_ip_port: client_ip_port,
	}
}
func (h *Header) split(id int) Header {
	dir_id   := h.packet_uuid.getDirID()
	new_uuid := dir_id + "." + strconv.Itoa(id)

	new_header := Header{
		session_id: h.session_id,
		packet_uuid: packetUuid{
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

const (
	// Max batch size es 8192 simplemente porque es el valor default de BUFSIZ en glibc:
	// https://sourceware.org/git/?p=glibc.git;a=blob;f=libio/stdio.h;h=e0e70945fab175fafcb0c8bbae96ad7eebe3df5a;hb=HEAD#l100
	// Ademas, en el tp0 el maximo era 8000, el cual es parecido en tamano
	MAX_BATCH_SIZE int = 8192
)

// Dado un paquete y un payload nuevo, te devulve todos los paquetes, subdivididos con ese payload nuevo
func NewPayloads(packet Packet, newpayload string) []Packet {
	return newPayloads(packet, newpayload, MAX_BATCH_SIZE)
}

func newPayloads(packet Packet, newpayload string, size int) []Packet {
	packet_amount_p := float64(len(newpayload)) / float64(size)
	packet_amount   := int(math.Ceil(packet_amount_p))

	packets := make([]Packet, packet_amount)


	is_eof := packet.IsEOF()
	// Si packet_amount = 3 -> 0, 1, 2
	// Si packet_amount = 2 -> 0, 1
	// Si packet_amount = 1 -> 0
	for batch := range packet_amount {
		begin     := batch * size
		end       := min(begin + size, len(newpayload))
		fmt.Printf("Begin: %d, End: %d \n", begin, end)
		content   := newpayload[begin:end]
		uuid      := packet.GetUUID()

		var packetUuid packetUuid
		if batch == len(packets) - 1 && is_eof {
			packetUuid = newPacketUuid(uuid, true)
		} else {
			packetUuid = newPacketUuid(uuid, false)
		}
		header := newHeader(packet.header.session_id, packetUuid, packet.header.client_ip_port)
		split_h := header.split(batch)

		packets[batch] = Packet {
			header: split_h,
			payload: content,
		}
	}


	return packets
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
		datasetsIDs[i] = strconv.FormatUint(datasetID, 10)
	}

	uuid := strings.Join(datasetsIDs, "-")
	eof  := pkt.IsEOF()
	packet_uuid := packetUuid {
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

func (p *Packet) GetUUID() string {
	return p.header.packet_uuid.uuid
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

func (p *Packet) GetSequenceNumber() int {
	uuid       := p.header.packet_uuid.uuid;
	uuid_split := strings.Split(uuid, ".")
	sequence_n, err := strconv.ParseInt(uuid_split[1], 10, 64)
	if err != nil {
		// Esto no deberia pasar porque nosotros construimos los headers con
		// valores bien conocidos. No son valores arbitrarios.
		panic(fmt.Errorf("Failed to get sequence number, %w", err))
	}

	return int(sequence_n)
}
