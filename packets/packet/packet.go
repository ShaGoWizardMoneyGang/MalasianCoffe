package packet

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"malasian_coffe/utils/dataset"
)

// Struct que asocia un paquete a enviar con la cola a la cual lo tiene que enviar

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
func (h *Header) append(id int) Header {
	new_uuid := h.packet_uuid.uuid + "." + strconv.Itoa(id)

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

// Mismo header, distinto payload
func ChangePayload(packet Packet, newpayload []string) []Packet {
	packets := make([]Packet, len(newpayload))

	for i, payload := range newpayload {
		newheader := packet.header

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

	// NOTA: Cuando hagamos packet splitting, solo devolver true en el ultimo paquete.
	eof         := true
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
		newheader = header.append(i)
		packets[i] = Packet{
			header:  newheader,
			payload: payload,
		}
	}
	return packets
}


// Devuelve packets que fueron el resultado de hacer un global aggregator
// - pkt es un paquete usado solo para extraer la metadata
// - dataset es el *NOMBRE* del dataset
// - newPayload es el contenido
func ChangePayloadGlobalAggregator(pkt Packet, datasetName string, newPayload []string) []Packet {
	datasetID, err := dataset.DatasetToID(datasetName)
	if err != nil {
		panic(fmt.Errorf("%s unknown dataset", datasetName))
	}
	datasetIDs := strconv.FormatUint(datasetID, 10)

	// NOTA: Cuando hagamos packet splitting, solo devolver true en el ultimo paquete.
	eof         := true
	packet_uuid := packetUuid {
		uuid: datasetIDs,
		eof: eof,
	};

	session_id := pkt.GetSessionID()
	clientAddr := pkt.GetClientAddr()

	header     := newHeader(session_id, packet_uuid, clientAddr)
	packets     := make([]Packet, len(newPayload))

	for i, payload := range newPayload {
		newheader := header
		newheader = header.append(i)
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

// Dado un paquete, y la cantidad de colas destino, te devuelve un string con su routing key
// devuelve un string
func GenerateRoutingKey(pkt Packet, queueAmount uint64) string {
	h := fnv.New64a()
	h.Write([]byte(pkt.GetSessionID()))
	hash := h.Sum64()

	// Modulo esta en el rango [0, queueAmount - 1]
	modulo := hash % queueAmount;

	modulo_s := strconv.FormatUint(modulo, 10)

	return modulo_s
}
