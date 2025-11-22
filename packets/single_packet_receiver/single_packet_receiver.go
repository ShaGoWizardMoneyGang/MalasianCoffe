package single_packet_receiver

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
	PACKET_WINDOW int = 50
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
type SinglePacketReceiver struct {

	// Packets que estan en la ventana actual. En cualquier momento, esto tiene
	// que ser HASTA PACKET_WINDOW.
	packets_in_window []packet.Packet

	// Funcion a aplicarle a los paquetes en la ventana. OJO, tiene que soportar ser parcial.
	transformer func(accumulated_input string, new_input string) string

	// // Determina si el buffer interno recibio todos los paquetes que esperaba.
	// windowFull bool

	// Para saber si llego el EOF
	EOF int

	identifier string

	path_resolver pathResolver

	logger logger

	windowFull bool

	checkpointer checkpointer
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
	LogFile
	Checkpoint
)

func (pr *pathResolver) resolve_path(file KnownFile) string {
	var path string
	switch file {
	case Root:
		path = pr.root
	case Metadata:
		path = pr.root + "/" + "metadata"
	case ReceivedEof:
		path = pr.root + "/" + "metada" + "/" + "received_eof"
	case ReceivedSqns:
		path = pr.root + "/" + "metada" + "/" + "received_sqns"
	case PartialWork:
		path = pr.root + "/" + "partial_work"
	case Packets:
		path = pr.root + "/" + "packets" + "/"
	case LogFile:
		path = pr.root + "/" + "window_log"
	case Checkpoint:
		path = pr.root + "/" + "checkpoint"
	// default:
	// 	panic(fmt.Sprintf("Unknown path %s", file))
	}
	return path
}

type checkpointMoment int
const (
	Cleaned checkpointMoment = iota
	LlegoElPaquete
	PreACK
	HiceACK
	PreFlushear
	LogAhead
	LaburoParcial
	LogBehind
)

func (c *checkpointMoment) toString() string {
	var repr string
	switch *c {
	case Cleaned:
		repr = "0CLEANED"
	case LlegoElPaquete:
		repr = "1LLEGOPAQUETE"
	case PreACK:
		repr = "2PRE-ACK"
	case HiceACK:
		repr = "3ACK"
	case PreFlushear:
		repr = "4PRE-FLUSH"
	case LogAhead:
		repr = "5LOGAHEAD"
	case LaburoParcial:
		repr = "6LABUROPARCIAL"
	case LogBehind:
		repr = "7LOGBEHIND"
	}
	return repr
}

// Estructura para debugear mejor el Momento en el tiempo en el que se murio la
// instancia.
type checkpointer struct {
	// Root de la CORRIENTE CORRIDA para marcar los checkpoints.
	checkpoint_root_current string
}

func newCheckpointer(checkpoint_root string) checkpointer {

	// Creo el directorio donde van a estar todos los directorios con los
	// checkpoints.
	if !disk.Exists(checkpoint_root) {
		disk.CreateDir(checkpoint_root)
	}

	entries, err := os.ReadDir(checkpoint_root)
	if err != nil {
		panic(err)
	}

	var amount_runs int
	for range entries {
		amount_runs += 1
	}
	amount_runs_s := strconv.FormatInt(int64(amount_runs), 10)
	checkpoint_root_current := checkpoint_root + "/" + amount_runs_s

	disk.CreateDir(checkpoint_root_current)

	return checkpointer{
		checkpoint_root_current: checkpoint_root_current,
	}
}

func (c *checkpointer) clean() {
	entries, err := os.ReadDir(c.checkpoint_root_current)
	if err != nil {
		panic(err)
	}
	for _, file := range entries {
		disk.DeleteFile(file.Name())
	}

	c.checkpoint(Cleaned)
}

func (c *checkpointer) checkpoint(checkpoint checkpointMoment) {
	file_name := checkpoint.toString()
	full_path := c.checkpoint_root_current + "/" + file_name
	disk.CreateFile(full_path)
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

	logger := newLogger("BORRAR", "BORRADO",
		pathResolver.resolve_path(ReceivedSqns),
		pathResolver.resolve_path(LogFile),
		pathResolver.resolve_path(Packets),
	)

	checkpointer_root := pathResolver.resolve_path(Checkpoint)
	checkpointer := newCheckpointer(checkpointer_root)

	return SinglePacketReceiver{
		packets_in_window:         packets_in_window,
		transformer:               transformer,
		EOF:                       received_eof,
		identifier:                identifier,
		path_resolver:             pathResolver,
		logger:                    logger,
		// TODO: Chequear que pasa si muero despues de recibir el ultimo paquete.
		windowFull:                false,
		checkpointer:              checkpointer,
	}
}

// Devuelve un booleano que representa si se recivieron todos los paquetes
// dentro de la ventana. Si este es el caso, se tienen que procesar.
func (pr *SinglePacketReceiver) ReceivePacket(pktMsg colas.PacketMessage) bool {
	defer pr.checkpointer.clean()
	// TODO: Chequear que pasa si muero despues de recibir el ultimo paquete.
	pkt := pktMsg.Packet
	pr.checkpointer.checkpoint(LlegoElPaquete)

	// fmt.Printf("Recibi %s\n", pkt.GetSequenceNumberString())

	// Guardo el paquete que acabo de recibir en disco
	{
		// NOTE: Por convencion, el nombre del archivo es su numero de secuencia
		pkt_file := pr.path_resolver.resolve_path(Packets) + pkt.GetSequenceNumberString()
		disk.AtomicWrite(pkt.Serialize(), pkt_file)
		if pkt.IsEOF() {
			pr.EOF = pkt.GetSequenceNumber()
			eof_sequence_number := pkt.GetSequenceNumberString()
			received_eof_file := pr.path_resolver.resolve_path(ReceivedEof)
			disk.AtomicWriteString(eof_sequence_number, received_eof_file)
		}
		// Como ya escribimos a disco, ackeamos
	}

	pr.checkpointer.checkpoint(PreACK)
	pktMsg.Message.Ack(false)
	pr.checkpointer.checkpoint(HiceACK)

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

	processed_sequence_number := pr.logger.get_processed_number()
	// En este buffer nos guardamos todos los numeros de secuencia recibido
	// para chequear que recibimos todos los paquetes (usado despues)
	received_packets := make([]int, len(processed_sequence_number) + len(pr.packets_in_window))

	var buffer strings.Builder
	for i, wind_pkt := range pr.packets_in_window {
		sq_n     := wind_pkt.GetSequenceNumber()
		// already_processed := slices.Contains(processed_sequence_number, sq_n)

		received_packets[i] = sq_n

		buffer.WriteString(wind_pkt.GetPayload())

		// NOTE: Esto es un healthcheck, mepa que mientras dos paquetes no
		// compartan header y tengan distinto payload, no deberia pasar nada malo.
		// if sq_n == pkt.GetSequenceNumber() || already_processed {
		// 	bitacora.Info(fmt.Sprintf("Duplicate packet received. UUID: %s", wind_pkt.GetUUID()))
		// }
	}

	amount_packets_in_window := len(pr.packets_in_window)

	// Chequeamos si recibi todos los paquetes
	offset := amount_packets_in_window
	for i, sq_n := range processed_sequence_number {
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
	do_flush_window := amount_packets_in_window >= PACKET_WINDOW
	if do_flush_window || allReceived {
		pr.checkpointer.checkpoint(PreFlushear)
		// LOG De todos los archivos que voy a borrar: Stage 1.
		// En la packet window: A, B, C
		// Voy a borrar A
		// Voy a borrar B
		// Voy a borrar C
		for _, packet := range pr.packets_in_window {
			sq_n := packet.GetSequenceNumberString()
			pr.logger.write_ahead(sq_n)
		}

		pr.checkpointer.checkpoint(LogAhead)
		// NOTE: Si se muere antes de escribir todos los "voy a borrar" en el
		// log, no pasa nada, porque cuando se levante de vuelta va a ver que
		// tiene en la ventana mas paquetes de los que esta anotada. Entonces,
		// solo tiene que anadir los paquetes que le faltan en el log.
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

		pr.checkpointer.checkpoint(LaburoParcial)

		// LOG De todos los archivos que borre: Stage 2.
		// En la packet window: A, B, C
		// Voy a borrar A
		// Voy a borrar B
		// Voy a borrar C
		// Borre A
		// Borre B
		// Borre C

		// NOTE: Si se muere antes de escribir todos los "borre", no pasa
		// nada. Porque cuando reviva va a ver que tiene mas "Voy a borrar"
		// que "Borre", entonces va a poder saber.
		for _, packet := range pr.packets_in_window {
			sq_n := packet.GetSequenceNumberString()
			// Aca tambien se borra el recurso asociado
			pr.logger.delete_behind(sq_n)
		}
		pr.checkpointer.checkpoint(LogBehind)
		// Ahora que la ventana esta procesada, y el cambio esta en disco,
		// actualizamos la memoria.
		pr.packets_in_window = nil
	}

	pr.windowFull = allReceived

	return allReceived
}

// Estructura encargada de escribir logs para trackear operaciones que tienen un
// par logico de "comienzo" y "fin".
type logger struct {
	// Path al log file.
	log_file string

	// Associated resource directory
	resource_dir string

	// Nombre de la operacion que marca el comienzo de la modicacion.
	write_operation string

	// Nombre de la operacion que marca el fin de la edicion.
	delete_operation string

	// Hashset de todos los numeros de sequencia recibidos y procesados hasta el
	// momento.
	processed_sequence_number []int

	// Log de todos los recursos procesados.
	processed_resource_log string

// ==================== CONSTRUCTED DURING INSTANTIATION =======================
	// Resources that are waiting to be "deleted_behind"
	pending_resources map[string] struct{}

	// Resources that are done and should not receive any modifications.
	done_resources map[string] struct{}
}


type operationType int
const (
	Write operationType = iota
	Delete
)

type log_entry struct {
	operation operationType
	resource string
}

// If it's not a write, it's a delete
func (le *log_entry) is_write() bool {
	return le.operation == Write
}

// Funcion que crea un logger.
// - log_file_path: Path al log file
// - write_operation: Nombre de la operacion que marca el comienzo de la edicion.
// - delete_operation: Nombre de la operacion que marca el fin de la edicion.
func newLogger(write_operation string, delete_operation string,
	received_sqns_file string,
	log_file_path      string,
	resouce_dir        string,
	) logger {

	write_op  := strings.ToUpper(write_operation)
	delete_op := strings.ToUpper(delete_operation)

	if !disk.Exists(received_sqns_file) {
		disk.CreateFile(received_sqns_file)
	}
	if !disk.Exists(log_file_path) {
		disk.CreateFile(log_file_path)
	}

	received_sqns_s, err := disk.Read(received_sqns_file)
	if err != nil {
		panic(err)
	}
	sqns := strings.Split(received_sqns_s, "\n")
	processed_sequence_numbers := []int{}
	for _, sqn := range sqns {
		if sqn == "" {
			continue
		}
		sqn_i, err := strconv.Atoi(sqn)
		if err != nil {
			panic(err)
		}

		already_added := slices.Contains(processed_sequence_numbers, sqn_i)
		if already_added {
			bitacora.Info(fmt.Sprintf("Duplicate packet in file. UUID: %s", sqn))
		}
		processed_sequence_numbers = append(processed_sequence_numbers, sqn_i)
	}


	logger := logger {
		log_file: log_file_path,
		resource_dir: resouce_dir,
		write_operation: write_op,
		delete_operation: delete_op,
		processed_sequence_number: processed_sequence_numbers,
		processed_resource_log: received_sqns_file,
		pending_resources: make(map[string]struct{}),
		done_resources: make(map[string]struct{}),
	}

	log_file, err := disk.Read(log_file_path)
	if err != nil {
		panic(err)
	}

	// Leo todos los logs que quedaron escritos para ya saber cual es el estado
	// actual.
	logs := strings.Split(log_file, "\n")
	for _, log := range logs {
		if log == "" {
			continue
		}
		log_entry := logger.parse_log_entry(log)
		resource  := log_entry.resource

		_, is_pending := logger.pending_resources[resource]
		_, is_done := logger.done_resources[resource]
		// If it's not a write, it's a delete.
		is_write   := log_entry.is_write()

		if is_write && !is_pending && !is_done {
			// Caso "basico" alguien escribio WRITE REC en el log.
			// Lo marco como pendiente de borrado.
			logger.pending_resources[resource] = struct{}{}
		} else if is_write && is_pending && !is_done {
			// Es un doble WRITE, esto es un error y no deberia pasar.
			panic(fmt.Sprintf("DOBLE WRITE DETECTED: %s", log_file_path))
		} else if is_write && is_pending && is_done {
			// Esto rompe una invariante. O esta en una tabla, o esta en la
			// otra.
			panic(fmt.Sprintf("LOG ESTA EN LAS DOS TABLAS %s", log_file_path))
		} else if is_write && !is_pending && is_done {
			// Esto es un recurso que fue logeado en el pasado, y ya termino.
			// TECNICAMENTE valido, pero no deberia suceder.
			bitacora.Info("Logger: Se detecto que un recurso que fue modificado y borrado en el pasado fue anadido de nuevo en el log.")
		} else if !is_write && is_pending && !is_done {
			// Caso tipico de que se escribio "borrado" en el log de un recurso.
			delete(logger.pending_resources, resource)
			logger.done_resources[resource] = struct{}{}

			// Tengo que fijarme si se borro el archivo.
			associated_file := logger.get_associate_file(resource)
			if disk.Exists(associated_file) {
				bitacora.Info(fmt.Sprintf("LOGGER: Encontre recurso que figuraba como borrado: %s. Lo borro.", associated_file))
				disk.DeleteFile(associated_file)
			}

			resource_i, err := strconv.Atoi(resource)
			if err != nil {
				panic(err)
			}

			// Si el paquete figura como borrado, pero no esta en la lista,
			// entonces signfica que el programa se cayo antes de appendear
			// el numero de paquete al log de paquetes procesados
			if !slices.Contains(processed_sequence_numbers, resource_i) {
				disk.AtomicAppend(resource, logger.processed_resource_log)
			}

		} else if !is_write && is_pending && is_done {
			// Esto rompe una invariante. O esta en una tabla, o esta en la
			// otra.
			panic(fmt.Sprintf("LOG ESTA EN LAS DOS TABLAS %s", log_file_path))
		} else if !is_write && !is_pending && is_done {
			// Es un doble WRITE, esto es un error y no deberia pasar.
			panic(fmt.Sprintf("DOBLE DELETE DETECTED: %s", log_file_path))
		} else if !is_write && !is_pending && !is_done {
			// Es un doble WRITE, esto es un error y no deberia pasar.
			panic(fmt.Sprintf("DELETE DE RECURSO NO WRITEADO DETECTADO: %s", log_file_path))
		}
	}


	return logger
}

func (l *logger) get_processed_number() []int {
	return l.processed_sequence_number
}

// Lee un entry de un log y te dice la log_entry que encontro. Principalmente
// esto es util para saber si es un operationType::Write o un
// operationType::Delete y el recurso modificado.
//
// NOTE: Todas los log entries son del tipo:
// <WRITE|DELETE> <RESOURCE>
//
// NOTE on a NOTE: WRITE|DELETE no se leen literalmente asi, depende de lo que
// se pase a write_operation y delete_operation en tiempo de creacion.
func (l *logger) parse_log_entry(log_entry_raw string) log_entry {
	log_entry_split := strings.Split(log_entry_raw, " ")
	if len(log_entry_split) > 2 {
		panic(fmt.Sprintf("Invalid log entry. Tried to split into 2, got split into: %d", len(log_entry_split)))
	}

	operation_s := log_entry_split[0]
	var operation operationType
	if operation_s == l.write_operation {
		operation = Write
	} else if operation_s == l.delete_operation {
		operation = Delete
	} else {
		panic(fmt.Sprintf("Invalid log entry. Expected %s or %s, got: %s", l.write_operation, l.delete_operation, operation_s))
	}

	resource := log_entry_split[1]

	return log_entry {
		operation: operation,
		resource: resource,
	}
}

// Indica al logger de loggear que [resource] va a ser modificado.
// WARNING: Por cada llamada a `write_ahead` tiene que haber una llamada a
// `delete_behind`
func (l *logger) write_ahead(resource string) {
	_, exists := l.pending_resources[resource]
	if exists {
		bitacora.Info(fmt.Sprintf("DOBLE WRITE DETECTED: %s, skipping write.", l.log_file))
		return
	}

	write := l.write_operation

	log_entry_s := write + " " + resource


	err := disk.AtomicAppend(log_entry_s, l.log_file)
	if err != nil {
		panic(err)
	}
	l.pending_resources[resource] = struct{}{}
}

func (l *logger) get_associate_file(resource string) string {
	associated_file        := l.resource_dir + resource
	return associated_file
}


// Indica al logger de loggear que [resource] fue modificado
// WARNING: Por cada llamada a `delete_behind` tiene que haber una llamada a
// `write_ahead`
func (l *logger) delete_behind(resource string) {
	associated_file        := l.get_associate_file(resource)

	_, marked_as_pending   := l.pending_resources[resource]
	_, marked_as_done      := l.done_resources[resource]
	associated_file_exists := disk.Exists(l.resource_dir + resource)


	if        marked_as_pending  && marked_as_done {
		// Esto no deberia suceder nunca. Ni siquiera es un error.
		panic(fmt.Sprintf("Resource %s found in both pending and done tables", resource))
	} else if marked_as_pending  && !marked_as_done {
		// Este es el caso canonico.

		// Si el archivo existe, significa que el programa se detuvo justo
		// antes de borrarlo. No pasa nada, is all good, lo borramos ahora.
		if associated_file_exists {
			delete_op   := l.delete_operation

			log_entry_s := delete_op + " " + resource

			disk.AtomicAppend(log_entry_s, l.log_file)

			// Si me muero aca, no pasa nada. Lo anado al revivir.
			disk.AtomicAppend(resource, l.processed_resource_log)
			resource_i, err := strconv.Atoi(resource)
			if err != nil {
				panic(err)
			}
			l.processed_sequence_number = append(l.processed_sequence_number, resource_i)


			// Si me muero aca, antes de borrarlo, no pasa nada porque va a
			// borrar el archivo al levantar el logger despues de morir.
			disk.DeleteFile(associated_file)
		} else {
			bitacora.Error(fmt.Sprintf("Recurso %s que figuraba como borrado existe en el filesystem", associated_file))
		}

		delete(l.pending_resources, resource)
		l.done_resources[resource] = struct{}{}
	} else if !marked_as_pending && marked_as_done {
		panic(fmt.Sprintf("LOGGER: Se pidio borrar un recurso que no estaba marcado como pendiente."))
	} else if !marked_as_pending && !marked_as_done {
		panic(fmt.Sprintf("LOGGER: Se pidio borrar un recurso que no estaba marcado como pendiente ni como listo (WTF?)."))
	}

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
// }

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
		panic("Invariante del Single Packet Receiver rota. Se trato de obtener el payload de un PacketReceiver que todavia no recibio todo.")
	}

	// TODO: Optimizacion: Si ya esta cargado en memoria, no lo vuelvo a leer
	// del disco.
	accumulated_work, err := disk.Read(pr.path_resolver.resolve_path(PartialWork))
	if err != nil {
		panic(err)
	}

	return accumulated_work
}
