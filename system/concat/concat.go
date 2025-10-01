package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	concat "malasian_coffe/system/concat/src"
	"malasian_coffe/system/middleware"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {
	colaEntrada, err := middleware.CreateQueue("FilteredTransactions1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(FilteredTransactions1): %w", err))
	}
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}

	colaSalida, err := middleware.CreateQueue("SalidaQuery1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(FilterMapper1YearAndAmount): %w", err))
	}

	worker := concat.Concat{}
	var result []packet.Packet
	for message := range *msgQueue {

		slog.Debug("Recibi mensaje")
		packet_reader := bytes.NewReader(message.Body)
		packet, err := packet.DeserializePackage(packet_reader)
		if err != nil {
			panic(fmt.Errorf("Error deserializing packet: %w", err))
		}

		slog.Debug("Procesando packet", "eof", packet.IsEOF(), "payload_len", len(packet.GetPayload()))
		result = worker.Process(packet)
		ackErr := message.Ack(false)
		if ackErr != nil {
			panic(fmt.Errorf("Could not ack, %w", ackErr))
		}
		if len(result) != 0 {
			slog.Info("Obtuve EOF, mando todo empaquetado a la cola de sending")
			for _, pkt := range result {
				err := colaSalida.Send(pkt.Serialize())
				if err != 0 {
					panic(fmt.Errorf("Error sending packet: %d", err))
				}
			}
		}
	}
}
