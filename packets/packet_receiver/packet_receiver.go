package packet_receiver

import (
	"bytes"
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/colas"
	"os"
	"strconv"
	"slices"
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
//   \_metadata/
//       \_received_eof
//       \_received_sqns
//   \_partial_work
//   \_packets/
//
// Funcionamiento:
// - En el directorio "metadata/" se va a guardar metadata del estado actual de los
//   paquetes recibidos.
//       - received_eof: si esta vacio, entonces no se recibio. Si tiene
//       contenido, es el sequence number del paquete que estaba marcado como
//       EOF.
//       - received_sqns: contiene una lista de todos los sequence numbers
//       recibidos hasta el momento.
//   paquetes recibidos.
// - En el archivo "partial_work" se guarda el procesamiento actual que se le
//   hizo a la ventana de paquetes. Despues de cada ventana, este archivo se carga
//   en memoria para agregar la informacion de los nuevos paquetes.
// - En el directorio "packets/" se guardan todos los paquetes recibidos que son
//   parte de la ventana actual, es decir, que todavia no fueron procesados.
type SinglePacketReceiver struct {

	// Packets que estan en la ventana actual. En cualquier momento, esto tiene
	// que ser HASTA PACKET_WINDOW
	packets_in_window []packet.Packet

	// Hashset de todos los numeros de sequencia recibidos y procesados hasta el
	// momento.
	// processed_sequence_number map[int]struct{}
	processed_sequence_number []int

	// Funcion a aplicarle a los paquetes en la ventana. OJO, tiene que soportar ser parcial.
	transformer func(accumulated_input string, new_input string) string

	// // Determina si el buffer interno recibio todos los paquetes que esperaba.
	// windowFull bool

	// Para saber si llego el EOF
	EOF int

	identifier string

	path_resolver pathResolver
}

type pathResolver struct {
	root string
}

func newPathResolver(root string) pathResolver {
	return pathResolver {
		root: root,
	}
}

type KnownFile int
const (
	Root KnownFile = iota
	Metadata
	ReceivedEof
	ReceivedSqns
	PartialWork
	Packets
)

func (pr *pathResolver) resolve_path(file KnownFile) string {
	var path string
	switch file {
	case Root:
		path = pr.root
	case Metadata:
		path = pr.root + "/" + "metada"
	case ReceivedEof:
		path = pr.root + "/" + "metada" + "/" + "received_eof"
	case ReceivedSqns:
		path = pr.root + "/" + "metada" + "/" + "received_sqns"
	case PartialWork:
		path = pr.root + "/" + "partial_work"
	case Packets:
		path = pr.root + "/" + "packets" + "/"
	// default:
	// 	panic(fmt.Sprintf("Unknown path %s", file))
	}
	return path
}

func NewSinglePacketReceiver(identifier string, transformer func(accumulated_input string, new_input string) string) SinglePacketReceiver {
	pathResolver := newPathResolver("packet_receiver" + "-" + identifier)
	packet_receiver_dir := pathResolver.resolve_path(Root)
	if !disk.Exists(packet_receiver_dir) {
		disk.CreateDir(packet_receiver_dir)
	}

	metada_dir := pathResolver.resolve_path(Metadata)
	if !disk.Exists(metada_dir) {
		disk.CreateDir(metada_dir)
	}
	received_eof_file := pathResolver.resolve_path(ReceivedEof)
	if !disk.Exists(received_eof_file) {
		disk.CreateFile(received_eof_file)
	}
	received_sqns_file := pathResolver.resolve_path(ReceivedSqns)
	if !disk.Exists(received_sqns_file) {
		disk.CreateFile(received_sqns_file)
	}

	partial_work_file := pathResolver.resolve_path(PartialWork)
	if !disk.Exists(partial_work_file) {
		disk.CreateFile(partial_work_file)
	}
	packets_dir := pathResolver.resolve_path(Packets)
	if !disk.Exists(packets_dir) {
		disk.CreateDir(packets_dir)
	}


	received_eof_s, err := disk.Read(received_eof_file)
	if err != nil {
		panic(err)
	}
	var received_eof int
	if received_eof_s == "" {
		// -1 representa si lo recibi o no
		received_eof = -1
	} else {
		received_eof_i64, err := strconv.ParseInt(received_eof_s, 10, 64)
		if err != nil {
			panic(err)
		}
		received_eof_i := int(received_eof_i64)

		received_eof = received_eof_i
	}


	received_sqns_s, err := disk.Read(received_eof_file)
	if err != nil {
		panic(err)
	}
	sqns := strings.Split(received_sqns_s, "\n")
	processed_sequence_numbers := []int{}
	for _, sqn := range sqns {
		sqn_i, err := strconv.Atoi(sqn)
		if err != nil {
			panic(err)
		}

		already_added := slices.Contains(processed_sequence_numbers, sqn_i)
		if already_added {
			bitacora.Info(fmt.Sprintf("Duplicate packet received. UUID: %s", sqn))
		}
		processed_sequence_numbers = append(processed_sequence_numbers, sqn_i)
	}

	packets_in_window := []packet.Packet{}
	entries, err := os.ReadDir(packets_dir)
	for _, file := range entries {
		packet_file, err := os.Open(packets_dir + "/" + file.Name())
		if err != nil {
			panic(err)
		}
		packet_serialized, err := os.ReadFile(packet_file.Name())
		if err != nil {
			panic(err)
		}

		packetReader := bytes.NewReader(packet_serialized)
		packet, err := packet.DeserializePackage(packetReader)

		packets_in_window = append(packets_in_window, packet)
	}

	return SinglePacketReceiver{
		packets_in_window:         packets_in_window,
		processed_sequence_number: processed_sequence_numbers,
		transformer:               transformer,
		// windowFull:                false,
		EOF:                       received_eof,
		identifier:                identifier,
		path_resolver:             pathResolver,
	}
}

// Devuelve un booleano que representa si se recivieron todos los paquetes
// dentro de la ventana. Si este es el caso, se tienen que procesar.
func (pr *SinglePacketReceiver) ReceivePacket(pktMsg colas.PacketMessage) bool {
	// TODO: Chequear que pasa si muero despues de recibir el ultimo paquete.
	pkt := pktMsg.Packet

	// Guardo el paquete que acabo de recibir en disco
	{
		// NOTE: Por convencion, el nombre del archivo es su numero de secuencia
		pkt_file := pr.path_resolver.resolve_path(Packets) + string(pkt.GetSequenceNumber())
		disk.AtomicWrite(pkt.Serialize(), pkt_file)
		if pkt.IsEOF() {
			pr.EOF = pkt.GetSequenceNumber()
			eof_sequence_number := string(pkt.GetSequenceNumber())
			received_eof_file := pr.path_resolver.resolve_path(ReceivedEof)
			disk.AtomicWriteString(eof_sequence_number, received_eof_file)
		}
		// Como ya escribimos a disco, ackeamos
	}

	pktMsg.Message.Ack(false)

	// Anado el paquete que acabo de recibir a la ventana de paquetes.

	// Si el programa se cae antes de anadirlo, no pasa porque se escribe en
	// disco. Cuando vuelva a iniciarse el programa, va a leer el archivo del
	// directorio packets y lo va a anadir en el array.
	pr.packets_in_window = append(pr.packets_in_window, pkt)


	// NOTE: Me parece que no hace falta ordernarlo, pero lo hago por las dudas.
	slices.SortFunc(pr.packets_in_window, func(i, j packet.Packet) int {
			sn_i := i.GetSequenceNumber()
			sn_j := j.GetSequenceNumber()
			return sn_i - sn_j
		})

	// En este buffer nos guardamos todos los numeros de secuencia recibido
	// para chequear que recibimos todos los paquetes (usado despues)
	received_packets := make([]int, len(pr.processed_sequence_number) + len(pr.packets_in_window))

	var buffer strings.Builder
	for i, wind_pkt := range pr.packets_in_window {
		sq_n     := wind_pkt.GetSequenceNumber()
		already_processed := slices.Contains(pr.processed_sequence_number, sq_n)

		received_packets[i] = sq_n

		buffer.WriteString(wind_pkt.GetPayload())

		// NOTE: Esto es un healthcheck, mepa que mientras dos paquetes no
		// compartan header y tengan distinto payload, no deberia pasar nada malo.
		// if sq_n == pkt.GetSequenceNumber() || already_processed {
		if already_processed {
			bitacora.Info(fmt.Sprintf("Duplicate packet received. UUID: %s", wind_pkt.GetUUID()))
		}
	}

	amount_packets_in_window := len(pr.packets_in_window)

	// Chequeamos si recibi todos los paquetes
	offset := amount_packets_in_window
	for i, sq_n := range pr.processed_sequence_number {
		received_packets[i + offset] = sq_n
	}

	// NOTE: Este si hace falta ordernarlo
	slices.Sort(received_packets)

	allReceived := true
	if pr.EOF == -1 {
		// Si ni me llego el EOF, entonces no hay chance de que haya
		// llegado todo
		allReceived = false
	}
	for i := 0; i < len(received_packets) && allReceived == true; i++ {
		// El ultimo paquete recibido tiene que si o si ser el ultimo
		if i == len(received_packets) - 1 {
			if i != pr.EOF {
				allReceived = false
			}

			// Como es el ultimo, hacemos early break para no pasarnos del
			// index con las siguientes comparaciones
			break
		}

		nxt_pkt_sn := received_packets[i + 1]

		pkt_sn := received_packets[i]

		if pkt_sn +1  != nxt_pkt_sn {
			allReceived = false
			break
		}
	}

	// Tengo que procesar la ventana en dos casos:
	// 1. Si la cantidad de paquetes excede la ventana, tengo que procesarlos
	//    para liberar la ventana y dar lugar a la proxima tanda.
	// 2. Si recibi todos los paquetes, entonces tambien tengo que procesar la
	//    ventana. Lo que puede pasar es que la ventana este llena a medias,
	//    pero como no van a llegar mas paquetes, la tengo que procesar ahora.
	if amount_packets_in_window >= int(PACKET_WINDOW) || allReceived {
		accumulated_work, err := disk.Read(pr.path_resolver.resolve_path(PartialWork))
		if err != nil {
			panic(err)
		}
		// Aplico la funcion transformer a todo lo que recibi + lo que acaba
		// de llegar.
		transformation := pr.transformer(accumulated_work, buffer.String())

		// Antes de escribir en disco, tengo que des-hacerme de los paquetes
		// la ventana.
		disk.AtomicWriteString(transformation, pr.path_resolver.resolve_path(PartialWork))
	}

	return allReceived


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

// Funcion que procesa los paquetes si lo ve necesario.
// func (pr *SinglePacketReceiver) processPackets() error {
// 	// NOTE: Me parece que no hace falta ordernarlo, pero lo hago por las dudas.
// 	slices.SortFunc(pr.packets_in_window, func(i, j packet.Packet) int {
// 			sn_i := i.GetSequenceNumber()
// 			sn_j := j.GetSequenceNumber()
// 			return sn_i - sn_j
// 		})


// 	// Buffer para chequear que recibimos todos los paquetes (usados despues)
// 	received_packets := make([]int, len(pr.processed_sequence_number) + len(pr.packets_in_window))

// 	var buffer strings.Builder
// 	for i, wind_pkt := range pr.packets_in_window {
// 		sq_n     := wind_pkt.GetSequenceNumber()
// 		already_processed := slices.Contains(pr.processed_sequence_number, sq_n)

// 		received_packets[i] = sq_n

// 		buffer.WriteString(wind_pkt.GetPayload())

// 		// NOTE: Esto es un healthcheck, mepa que mientras dos paquetes no
// 		// compartan header y tengan distinto payload, no deberia pasar nada malo.
// 		// if sq_n == pkt.GetSequenceNumber() || already_processed {
// 		if already_processed {
// 			bitacora.Info(fmt.Sprintf("Duplicate packet received. UUID: %s", wind_pkt.GetUUID()))
// 		}
// 	}

// 	// Si la cantidad de paquetes excede la ventana, tengo que procesarlos.
// 	if len(pr.packets_in_window) >= int(PACKET_WINDOW) {
// 		accumulated_work, err := disk.Read(pr.path_resolver.resolve_path(PartialWork))
// 		if err != nil {
// 			return err
// 		}
// 		transformation := pr.transformer(accumulated_work, buffer.String())

// 		disk.AtomicWriteString(transformation, pr.path_resolver.resolve_path(PartialWork))
// 	}

// 	// Chequeamos si recibi todos los paquetes
// 	offset := len(pr.packets_in_window)
// 	for i, sq_n := range pr.processed_sequence_number {
// 		received_packets[i + offset] = sq_n
// 	}

// 	// NOTE: Este si hace falta ordernarlo
// 	slices.Sort(received_packets)

// 	allReceived := true
// 	if pr.EOF == nil {
// 		// Si ni me llego el EOF, entonces no hay chance de que haya
// 		// llegado todo
// 		allReceived = false
// 	} 
// 	for i := 0; i < len(received_packets) && allReceived == true; i++ {
// 		// El ultimo paquete recibido tiene que si o si ser el ultimo
// 		if i == len(received_packets) - 1 {
// 			if uint(i) != *pr.EOF {
// 				allReceived = false
// 			}

// 			// Como es el ultimo, hacemos early break para no pasarnos del
// 			// index con las siguientes comparaciones
// 			break
// 		}

// 		nxt_pkt_sn := received_packets[i + 1]

// 		pkt_sn := received_packets[i]

// 		if pkt_sn +1  != nxt_pkt_sn {
// 			allReceived = false
// 			break
// 		}
// 	}

// 	return allReceived
// }

// func (pr *SinglePacketReceiver) ReceivedAll() bool {
// 	return pr.windowFull
// }


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
