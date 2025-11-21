package packet_receiver

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/utils/colas"
	"malasian_coffe/packets/packet"
	"slices"
	"strings"
)

type PacketReceiver struct {
	// received_packages map[string]Packet
	ordered_package []packet.Packet

	// Determina si el buffer interno recibio todos los paquetes que esperaba.
	allReceived bool

	// Para saber si llego el EOF
	receivedEOF bool

	buffer strings.Builder

	humanIdentifier string
}

// HumanIdentifier es el nombre que identifica al packet receiver. Esto esta
// pensando para los logs.  Esta pensando para que sea un nombre que nos ayude a
// la hora de leer los prints del log.
func NewPacketReceiver(humanIdentifier string) PacketReceiver {
	return PacketReceiver{
		ordered_package: []packet.Packet{},
		receivedEOF:     false,
		allReceived:     false,
		humanIdentifier: humanIdentifier,
	}
}

// Devuelve un booleano que representa si se recivieron todos los paquetes
// esperados o no.
func (pr *PacketReceiver) ReceivePacket(pktMsg colas.PacketMessage) bool {
	// Apenas llega un nuevo paquete, el buffer queda invalido. Reseteamos.
	pr.buffer.Reset()

	pkt := pktMsg.Packet

	n, exits := slices.BinarySearchFunc(pr.ordered_package, pkt,
		func(i, j packet.Packet) int {
			sn_i := i.GetSequenceNumber()
			sn_j := j.GetSequenceNumber()
			return sn_i - sn_j
		})

	if exits {
		bitacora.Debug(fmt.Sprintf("Duplicate packet received. UUID: %s", pkt.GetUUID()))
		pkt_existente := pr.ordered_package[n]
		if pkt_existente.GetPayload() != pkt.GetPayload() {
			bitacora.Info(fmt.Sprintf(`ATENCION: Los dos paquetes tienen distinto payload.
Existente:
%s
Nuevo:
%s
`, pkt_existente.GetPayload(), pkt.GetPayload()))
		}
	} else {
		pr.ordered_package = slices.Insert(pr.ordered_package, n, pkt)
	}

	// Una vez insertado en el slice, lo podemos marcar como recibido.
	// TODO: Esto no se esta guardando en disco todavia, hay que incluirlo
	// Ver issue: https://github.com/ShaGoWizardMoneyGang/MalasianCoffe/issues/123
	pktMsg.Message.Ack(false)

	// Solamente actualizamos esto si no recibimos el EOF hasta ahora.
	if !pr.receivedEOF {
		pr.receivedEOF = pkt.IsEOF()
	}

	// Early check, si no llego el EOF, entonces es imposible que este
	// completo.  Entonces ni nos calentamos en construir el buffer porque ya
	// sabemos que va a estar mal.
	// NOTE: No poner este if adentro del de arriba, tenemos que chequear dos
	// veces este valor: Una para saber si lo tenemos que actualizar y otra
	// para saber si ya lo recibimos.
	if !pr.receivedEOF {
		return false
	}

	allReceived := true
	for i, pkt := range pr.ordered_package {
		pr.buffer.WriteString(pkt.GetPayload())

		// Llegue al ultimo packet, tiene que ser el EOF si o si.
		if i == len(pr.ordered_package)-1 {
			allReceived = pkt.IsEOF()
			break
		}

		nxt_pkt := pr.ordered_package[i+1]
		nxt_pkt_sn := nxt_pkt.GetSequenceNumber()

		pkt_sn := pkt.GetSequenceNumber()

		if pkt_sn+1 != nxt_pkt_sn {
			allReceived = false
			break
		}
	}

	pr.allReceived = allReceived

	// Para no usar memoria al cohete, si no esta todo recibido, tambien
	// reseteamos.
	if pr.allReceived == false {
		pr.buffer.Reset()
	} else {
		bitacora.Info(fmt.Sprintf("El packet receiver %s, recibio todos los paquetes que esperaba. El tamano es de: %d", pr.humanIdentifier, len(pr.ordered_package)))
	}

	return pr.allReceived
}

func (pr *PacketReceiver) ReceivedAll() bool {
	return pr.allReceived
}

// Devuelve el packet acumulado.
func (pr *PacketReceiver) GetPayload() string {
	if pr.allReceived != true {
		// NOTE: No borrar este panic. Es importante que si en algun momento
		// se rompe la invariante, que el programa explote para poder debugear
		// mejor.
		// Un error no lo solucionaria porque esos son ignorables.
		panic("Invariante del Packet Receiver rota. Se trato de obtener el payload de un PacketReceiver que todavia no recibio todo.")
	}
	return pr.buffer.String()
}
