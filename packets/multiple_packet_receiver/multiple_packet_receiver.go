package multiple_packet_receiver

import (
	"bytes"
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
	"malasian_coffe/utils/disk"
	"os"
	"slices"
	"strconv"
	"strings"
)

// =============================================================================
// ===                                                                       ===
// ===                                                                       ===
// ===   ___  ______ _____    ______ _   _______ _     _____ _____   ___     ===
// ===  / _ \ | ___ \_   _|   | ___ \ | | | ___ \ |   |_   _/  __ \ / _ \    ===
// === / /_\ \| |_/ / | |     | |_/ / | | | |_/ / |     | | | /  \// /_\ \   ===
// === |  _  ||  __/  | |     |  __/| | | | ___ \ |     | | | |    |  _  |   ===
// === | | | || |    _| |_    | |   | |_| | |_/ / |_____| |_| \__/\| | | |   ===
// === \_| |_/\_|    \___/    \_|    \___/\____/\_____/\___/ \____/\_| |_/   ===
// ===                                                                       ===
// ===                                                                       ===
// ===                                                                       ===
// =============================================================================

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
//       \_packet-1
//       \_packet-2
//       \_packet-3
//       \_packet-4
//       \_packet-5
//   \_window_log
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
// - El archivo "window_log" guarda el log de los paquetes procesados.

// Nombre del dataset que origino estos datos.
type NombreDataset string;

// Todo el contenido recibido.
type ContenidoCompleto string;

type MultiplePacketReceiver struct {
	// Session UUID
	identifier string

	// Lista de los receivers de los distintos datasets
	receivers []datasetReceiver

	// Funcion transformer a aplicar. El mapa que se le pasa por parametro tiene
	// la siguiente forma:
	// {
	//     <nombre_dataset>: contenido_completo del dataset
	// }
	transformer func(inputs map[NombreDataset] ContenidoCompleto) string

	// Ultimo paquete de la session. Este lo guardamos porque damos el ultimo
	// ACK con la funcion de clean.
	// OJO AL PIOJO: Arranca como basura hasta que se obtiene el ultimo paquete.
	last_packet colas.PacketMessage

	// Indica que se recibieron todos los datasets, se les aplico la funcion de
	// transformacion y se enviaron al siguiente worker.
	// Usado al final para ver si se tiene que enviar el trabajo procesado de
	// nuevo.
	allReceived bool
}



func NewMultiplePacketReceiver(
	identifier string,
	expected_datasets []NombreDataset,
	transformer func(inputs map[NombreDataset] ContenidoCompleto) string,
) MultiplePacketReceiver {
	receivers := make([]datasetReceiver, len(expected_datasets))

	for i := 0; i < len(expected_datasets); i++ {
		name := expected_datasets[i]
		new_receiver := newDatasetReceiver(identifier, name)
		receivers[i] = new_receiver
	}
	// Crea un directorio con el nombre de la session
	multiple_packet_receiver := MultiplePacketReceiver{
		identifier:                identifier,
		receivers:                 receivers,
		transformer:               transformer,
		allReceived:               false,
	}

	return multiple_packet_receiver
}

// Devuelve un booleano que representa si se recivieron todos los paquetes
// dentro de la ventana. Si este es el caso, se tienen que procesar.
func (pr *MultiplePacketReceiver) ReceivePacket(pktMsg colas.PacketMessage) bool {
	// Antes de empezar, me fijo si ya termine. SI ya termine pero llegue
	// hasta aca, signfica que alguno de los packet receivers no llego a
	// enviar ACK porque murio la computadora antes.

	pkt := pktMsg.Packet

	sent := false
	for i := 0; i < len(pr.receivers); i++ {
		receiver := &pr.receivers[i]

		if !receiver.canReceivePacket(&pkt) {
			continue
		}

		// Si me muero antes de enviarlo, no pasa nada porque al revivir, lo
		// voy a volver a enviar.
		// Le enviamos el pauete al recibir que lo necesite.
		receiver.receivePacket(pkt)

		sent = true

		// Si me muero despues de recibirlo, no pasa nada porque el receiver
		// ya guardo el paquete en disco e hizo ACK del paquete.
	}

	// Sanity check
	if sent == false {
		dataset_name := "Couldn't get name"
		packet_id, err := strconv.ParseUint(pktMsg.Packet.GetDirID(), 10, 64)
		if err == nil {
			dataset_name_local, err := dataset.IDtoDataset(packet_id)
			if err == nil {
				dataset_name = dataset_name_local
			}
		}
		panic(fmt.Sprintf("Failed to send pkt %s to any receiver. It contained dataset %s.", pktMsg.Packet.GetUUID(), dataset_name))
	}

	allReceived := true
	for i := 0; i < len(pr.receivers) && allReceived == true; i++ {
		curr_receiver := pr.receivers[i]
		is_done := curr_receiver.checkIfReceivedAll()

		// Si cualquiera es falso, este for loop corta
		allReceived = allReceived && is_done
	}

	if allReceived {
		pr.last_packet = pktMsg
	} else {
		pktMsg.Message.Ack(false)
	}

	pr.allReceived = allReceived

	return allReceived
}


// Funcion que destruye todos los archivos creados por el SinglePacketReceiver
func (pr *MultiplePacketReceiver) Clean() {
	// Antes de borrar, tenemos que ACKEAR. Sino, podria pasar de borrar todo,
	// morir, y cuando nos re-envian el ultimo paquete, no sabemos que ya
	// terminamos.
	pr.last_packet.Message.Ack(false)

	for i := 0; i < len(pr.receivers); i++ {
		receiver := &pr.receivers[i]

		receiver.clean()
	}
}

// Devuelve el resultado de aplicar la funcion
func (pr *MultiplePacketReceiver) GetPayload() string {
	if pr.allReceived != true {
		// NOTE: No borrar este panic. Es importante que si en algun momento
		// se rompe la invariante, que el programa explote para poder debugear
		// mejor.
		// Un error no lo solucionaria porque esos son ignorables.
		panic("Invariante del Single Packet Receiver rota. Se trato de obtener el payload de un PacketReceiver que todavia no recibio todo.")
	}

	dataset_map := make(map[NombreDataset] ContenidoCompleto, len(pr.receivers))
	for i := 0; i < len(pr.receivers); i++ {
		receiver        :=     &pr.receivers[i]
		dataset_name    := receiver.getDatasetName()
		dataset_content := receiver.getReceivedPackets()
		dataset_map[dataset_name] = ContenidoCompleto(dataset_content)
	}

	finished_work := pr.transformer(dataset_map)

	return finished_work
}


// =============================================================================
// ===                                                                       ===
// ===                                                                       ===
// ===                      _     ___________                                ===
// ===                     | |   |_   _| ___ \                               ===
// ===                     | |     | | | |_/ /                               ===
// ===                     | |     | | | ___ \                               ===
// ===                     | |_____| |_| |_/ /                               ===
// ===                     \_____/\___/\____/                                ===
// ===                                                                       ===
// ===                                                                       ===
// ===                                                                       ===
// =============================================================================

// Libreria interna del multiplepacket receiver.

// ============================ DATASET RECEIVER ===============================

type datasetReceiver struct {
	// Directorio que contiene los recursos asociados a este dataset.
	datasetName NombreDataset

	// Array de todos los numeros de sequencia recibidos y procesados hasta el
	// momento.
	received_sequence_numbers []int

	// Para saber si llego el EOF
	EOF int

	// NOTE: Usado solo como sanity check
	receivedAll bool

	path_resolver datasetPathResolver
}

func newDatasetReceiver(identifier string, datasetName NombreDataset) datasetReceiver {
	pathResolver := newDatasetPathResolver(identifier, datasetName)

	metada_dir := pathResolver.resolve_path(metadata)
	if !disk.Exists(metada_dir) {
		disk.CreateDir(metada_dir)
	}
	received_eof_file := pathResolver.resolve_path(receivedEof)
	if !disk.Exists(received_eof_file) {
		disk.CreateFile(received_eof_file)
	}

	packets_dir := pathResolver.resolve_path(packets)
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


	// Anadimos los seq numbers de todos los paquetes recibidos
	received_sequence_numbers := []int{}
	entries, err := os.ReadDir(packets_dir)
	for _, file := range entries {
		packet_file, err := os.Open(packets_dir + "/" + file.Name())
		if err != nil {
			panic(err)
		}

		file_name := packet_file.Name()
		packet_sq_num, err := strconv.ParseInt(file_name, 10, 64)
		if err != nil {
			panic(err)
		}

		received_sequence_numbers = append(received_sequence_numbers, int(packet_sq_num))
	}

	// TODO: Chequear disco
	receiver := datasetReceiver {
		datasetName: datasetName,
		received_sequence_numbers: received_sequence_numbers,
		EOF                      : received_eof,
		receivedAll: false,
		path_resolver: pathResolver,
	 }

	return receiver
}

// Te dice si recibe ese tipo de paquete
func (dr *datasetReceiver) canReceivePacket(pkt *packet.Packet) bool {
	packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
	if err != nil {
		panic("Invalid packet id")
	}
	dataset_name, err := dataset.IDtoDataset(packet_id)
	if err != nil {
		panic("Failed to translate packet id to dataset name")
	}

	my_dataset_name := string(dr.datasetName)
	can_receive := dataset_name == my_dataset_name

	return can_receive
}

// Recibe un paquete y lo guarda en disco si no lo tenia antes.
func (dr *datasetReceiver) receivePacket(pkt packet.Packet) {
	if !dr.canReceivePacket(&pkt) {
		panic("Dataset receiver recibio paquete que no le correspondia.")
	}


	// fmt.Printf("Recibi %s\n", pkt.GetSequenceNumberString())
	allreadyProcessed := dr.checkIfAlreadyProcessed(pkt.GetSequenceNumber())

	if !allreadyProcessed {
	   // Recibo paquete, lo guardo en disco SI no lo recibi antes.
	   pkt_file := dr.path_resolver.resolve_path(packets) + pkt.GetSequenceNumberString()
	   disk.AtomicWrite(pkt.Serialize(), pkt_file)

	   if pkt.IsEOF() {
		  dr.EOF = pkt.GetSequenceNumber()
		  eof_sequence_number := pkt.GetSequenceNumberString()
		  received_eof_file := dr.path_resolver.resolve_path(receivedEof)
		  disk.AtomicWriteString(eof_sequence_number, received_eof_file)
	   }
	}

	if !allreadyProcessed {
		// Ahora, como lo guardamos en disco, anadimo el ID a la lista de paquetes
		// recibidos.
		dr.received_sequence_numbers = append(dr.received_sequence_numbers, pkt.GetSequenceNumber())
	}
}

func (dr *datasetReceiver) checkIfReceivedAll() bool {
	// Si ni siquiera recibimos el EOF, entonces no hay chance de que hayamos
	// recibido todo.
	if dr.EOF == -1 {
		return false
	}

	// Incluso con el EOF, puede que lleguen desordenados. Asique los ordenamos.
	slices.Sort(dr.received_sequence_numbers)

	// Como los acabamos de ordenar, si recibimos todos los paquetes, deberian
	// estar ordenamos los IDs.
	allReceived := true
	for i, pktSN := range dr.received_sequence_numbers {

		// Llegue al ultimo packet, tiene que ser el EOF si o si.
		if i == len(dr.received_sequence_numbers) - 1 {
			allReceived = pktSN == dr.EOF
			break
		}

		nxt_pkt_sn  := dr.received_sequence_numbers[i + 1]

		// Mi sq + 1 tiene que ser igual al siguiente, sino, no llego todo.
		if pktSN + 1 != nxt_pkt_sn {
			allReceived = false
			break
		}
	}

	dr.receivedAll = allReceived
	if allReceived == true {
		message := fmt.Sprintf("El packet receiver %s, recibio todos los paquetes que esperaba. El tamano es de: %d", dr.datasetName, len(dr.received_sequence_numbers))
		bitacora.Info(message)
	}

	return dr.receivedAll
}

// Retorna todos los paquetes recibidos SI estan todos.
func (dr *datasetReceiver) getReceivedPackets() string {
	if !dr.checkIfReceivedAll() {
		panic(fmt.Sprintf("Invariante rota de %s. Solo es valido obtener los paquetes recibidos si recibi todo.", dr.datasetName))
	}

	full_payload := dr.readStoredPackets()

	return full_payload
}

// Lee todos los paquetes guardados en disco y devuelve su contenido
func (dr *datasetReceiver) readStoredPackets() string {
	var buffer strings.Builder

	packets_dir := dr.path_resolver.resolve_path(packets)
	entries, err := os.ReadDir(packets_dir)
	if err != nil {
		panic(err)
	}
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

		payload := packet.GetPayload()
		buffer.WriteString(payload)
	}

	return buffer.String()
}

// ============================= PATH RESOLVER ================================

// Estructura para centralizar la resolucion de paths y aprovechar el sistema de
// tipos para asegurar que no se mezclan los paths con otra cosa.
type datasetPathResolver struct {
	root string
}

// root es el directorio que todos los packet_receiver usan en comun.
func newDatasetPathResolver(session_id string, dataset_name NombreDataset) datasetPathResolver {
	root := "packet_receiver" + "/" + session_id
	if !disk.Exists(root) {
		disk.CreateDir(root)
	}

	root_with_dataset := root + "/" + string(dataset_name)
	if !disk.Exists(root_with_dataset) {
		disk.CreateDir(root_with_dataset)
	}

	return datasetPathResolver {
		root: root_with_dataset,
	}
}

type knownFile int
const (
	root knownFile = iota
	metadata
	receivedEof
	packets
)

func (pr *datasetPathResolver) resolve_path(file knownFile) string {
	var path string
	switch file {
	case root:
		path = pr.root
	case metadata:
		path = pr.root + "/" + "metadata"
	case receivedEof:
		path = pr.resolve_path(metadata) + "/" + "received_eof"
	case packets:
		// Le pongo un 0 para que sea lo primero que aparece
		path = pr.root + "/" + "0packets" + "/"
	}
	return path
}

func (dr *datasetReceiver) checkIfAlreadyProcessed(resourceId int) bool {
	alreadyProcessed := false
	for i := 0; i < len(dr.received_sequence_numbers) && alreadyProcessed == false ; i++ {
		curr_id := dr.received_sequence_numbers[i]
		alreadyProcessed = curr_id == resourceId
	}

	return alreadyProcessed
}

func (dr *datasetReceiver) getDatasetName() NombreDataset {
	return dr.datasetName
}

func (dr *datasetReceiver) clean() {
	path := dr.path_resolver.resolve_path(root)

	disk.DeleteDirRecursively(path)
}
