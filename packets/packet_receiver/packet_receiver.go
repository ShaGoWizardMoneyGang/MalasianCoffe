package packet_receiver

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/utils/colas"
	"malasian_coffe/packets/packet"

	"malasian_coffe/utils/disk"
	"strings"
)

const (
	PACKET_WINDOW uint = 50
)

// Este Packet Receiver esta pensado para workers que solo reciben un tipo de paquete,
// como los global aggregators y el concater.
// Cada Packet Receiver va a almacenar los datos en un directorio propio.
// Ese directorio tiene la siguiente estructura:
//
// packet_receiver-<identifier>/
//   \_metadata
//   \_partial_work
//   \_packets/
//
// Funcionamiento:
// - En el archivo "metadata" se va a guardar metadata del estado actual de los
//   paquetes recibidos. Principalmente si se recibio el EOF o no, los IDS de los
//   paquetes recibidos.
// - En el archivo "partial_work" se guarda el procesamiento actual que se le
//   hizo a la ventana de paquetes. Despues de cada ventana, este archivo se carga
//   en memoria para agregar la informacion de los nuevos paquetes.
// - En el directorio "packets" se guardan todos los paquetes recibidos que son
//   parte de la ventana actual, es decir, que todavia no fueron procesados.
type SinglePacketReceiver struct {

	// Packets que estan en la ventana actual. En cualquier momento, esto tiene
	// que ser HASTA PACKET_WINDOW
	packets_in_window []packet.Packet

	// Hashset de todos los numeros de sequencia recibidos y procesados hasta el
	// momento.
	processed_sequence_number map[int]struct{}

	// Funcion a aplicarle a los paquetes en la ventana. OJO, tiene que soportar ser parcial.
	transformer func(accumulated_input string)

	// Determina si el buffer interno recibio todos los paquetes que esperaba.
	windowFull bool

	// Para saber si llego el EOF
	receivedEOF bool

	buffer strings.Builder

	identifier string
}

func NewSinglePacketReceiver(identifier string, transformer func(accumulated_input string)) SinglePacketReceiver {

	packet_receiver_dir := "packet_receiver" + "-" + identifier
	if !disk.Exists(packet_receiver_dir) {
		disk.CreateDir(packet_receiver_dir)
	}
	metada_file := packet_receiver_dir + "/metada"
	if !disk.Exists(metada_file) {
		disk.CreateFile(metada_file)
	}
	partial_work_file := packet_receiver_dir + "/partial_work"
	if !disk.Exists(partial_work_file) {
		disk.CreateFile(partial_work_file)
	}
	packets_dir := packet_receiver_dir + "/packets"
	if !disk.Exists(packets_dir) {
		disk.CreateDir(packets_dir)
	}

	// TODO: Chequear si ya existen los archivos.
	return SinglePacketReceiver{
		packets_in_window: []packet.Packet{},
		receivedEOF:     false,
		windowFull:     false,
		identifier: identifier,
		transformer: transformer,
	}
}

// Devuelve un booleano que representa si se recivieron todos los paquetes
// dentro de la ventana. Si este es el caso, se tienen que procesar.
func (pr *SinglePacketReceiver) ReceivePacket(pktMsg colas.PacketMessage) bool {
	// Apenas llega un nuevo paquete, el buffer queda invalido. Reseteamos.
	pr.buffer.Reset()

	pkt := pktMsg.Packet

	// NOTE: Esto es un healthcheck, mepa que mientras dos paquetes no
	// compartan header y tengan distinto payload, no deberia pasar nada malo.
	for _, wind_pkt := range pr.packets_in_window {
		sq_n     := wind_pkt.GetSequenceNumber()
		_, already_processed := pr.processed_sequence_number[pkt.GetSequenceNumber()]
		if sq_n == pkt.GetSequenceNumber() || already_processed {
			bitacora.Info(fmt.Sprintf("Duplicate packet received. UUID: %s", pkt.GetUUID()))
		}
	}

	pr.packets_in_window = append(pr.packets_in_window, pkt)

	if len(pr.packets_in_window) >= int(PACKET_WINDOW) {
		pr.windowFull = true
		return true
	}


// 	n, exits := slices.BinarySearchFunc(pr.packets_in_window, pkt,
// 		func(i, j packet.Packet) int {
// 			sn_i := i.GetSequenceNumber()
// 			sn_j := j.GetSequenceNumber()
// 			return sn_i - sn_j
// 		})

// 	if exits {
// 		bitacora.Debug(fmt.Sprintf("Duplicate packet received. UUID: %s", pkt.GetUUID()))
// 		pkt_existente := pr.packets_in_window[n]
// 		if pkt_existente.GetPayload() != pkt.GetPayload() {
// 			bitacora.Info(fmt.Sprintf(`ATENCION: Los dos paquetes tienen distinto payload.
// Existente:
// %s
// Nuevo:
// %s
// `, pkt_existente.GetPayload(), pkt.GetPayload()))
// 		}
// 	} else {
// 		pr.packets_in_window = slices.Insert(pr.packets_in_window, n, pkt)
// 	}

// 	// Una vez insertado en el slice, lo podemos marcar como recibido.
// 	// TODO: Esto no se esta guardando en disco todavia, hay que incluirlo
// 	// Ver issue: https://github.com/ShaGoWizardMoneyGang/MalasianCoffe/issues/123
// 	pktMsg.Message.Ack(false)

// 	// Solamente actualizamos esto si no recibimos el EOF hasta ahora.
// 	if !pr.receivedEOF {
// 		pr.receivedEOF = pkt.IsEOF()
// 	}

// 	// Early check, si no llego el EOF, entonces es imposible que este
// 	// completo.  Entonces ni nos calentamos en construir el buffer porque ya
// 	// sabemos que va a estar mal.
// 	// NOTE: No poner este if adentro del de arriba, tenemos que chequear dos
// 	// veces este valor: Una para saber si lo tenemos que actualizar y otra
// 	// para saber si ya lo recibimos.
// 	if !pr.receivedEOF {
// 		return false
// 	}

// 	allReceived := true
// 	for i, pkt := range pr.packets_in_window {
// 		pr.buffer.WriteString(pkt.GetPayload())

// 		// Llegue al ultimo packet, tiene que ser el EOF si o si.
// 		if i == len(pr.packets_in_window)-1 {
// 			allReceived = pkt.IsEOF()
// 			break
// 		}

// 		nxt_pkt := pr.packets_in_window[i+1]
// 		nxt_pkt_sn := nxt_pkt.GetSequenceNumber()

// 		pkt_sn := pkt.GetSequenceNumber()

// 		if pkt_sn+1 != nxt_pkt_sn {
// 			allReceived = false
// 			break
// 		}
// 	}

// 	pr.allReceived = allReceived

// 	// Para no usar memoria al cohete, si no esta todo recibido, tambien
// 	// reseteamos.
// 	if pr.allReceived == false {
// 		pr.buffer.Reset()
// 	} else {
// 		bitacora.Info(fmt.Sprintf("El packet receiver %s, recibio todos los paquetes que esperaba. El tamano es de: %d", pr.identifier, len(pr.packets_in_window)))
// 	}

// 	return pr.allReceived
}

func (pr *SinglePacketReceiver) ReceivedAll() bool {
	return pr.windowFull
}

// Devuelve el packet acumulado.
func (pr *SinglePacketReceiver) GetPayload() string {
	if pr.windowFull != true {
		// NOTE: No borrar este panic. Es importante que si en algun momento
		// se rompe la invariante, que el programa explote para poder debugear
		// mejor.
		// Un error no lo solucionaria porque esos son ignorables.
		panic("Invariante del Packet Receiver rota. Se trato de obtener el payload de un PacketReceiver que todavia no recibio todo.")
	}
	return pr.buffer.String()
}
