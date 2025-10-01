package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	aggregator "malasian_coffe/system/global_aggregator/src"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"os"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {

	aggregatorFunction := os.Args[2]
	if len(aggregatorFunction) == 0 {
		panic("No filter function provided")
	}
	rabbitAddr := os.Args[1]

	switch aggregatorFunction {
	case "query3":
		println("[GORDO AGGREGATOR QUERY3]")
		colaEntrada, err := middleware.CreateQueue("FilteredTransactions3", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
		if err != nil {
			panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
		}
		msgQueue, consumeError := colaEntrada.StartConsuming()
		if consumeError != 0 {
			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
		}

		colaSalida, err := middleware.CreateQueue("PartialAggregations3", middleware.ChannelOptionsDefault())
		if err != nil {
			panic(fmt.Errorf("CreateQueue(PartialAggregations3): %w", err))
		}

		worker := aggregator.Aggregator{}
		var result []packet.Packet
		for message := range *msgQueue {

			slog.Debug("Recibi mensaje")
			packet_reader := bytes.NewReader(message.Body)
			packet, _ := packet.DeserializePackage(packet_reader)

			println("Pkt enviado al aggregator:", packet.GetPayload())
			result = worker.Process(packet, "agregator3ByMonthTPV")
			println("Resultado de aggregator:", result[0].GetPayload())
			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
			if len(result) != 0 {
				slog.Info("Obtuve EOF, mando todo empaquetado a la cola de sending")
				for _, pkt := range result {
					err := colaSalida.Send(pkt.Serialize())
					println(err)
				}
			}
		}
	case "query3Global":
		println("[GORDO AGGREGATOR QUERY3 GLOBAL]")
		colaEntrada, err := middleware.CreateQueue("PartialAggregations3", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
		if err != nil {
			panic(fmt.Errorf("CreateQueue(COLA PartialAggregations3 ENTRADA): %w", err))
		}
		msgQueue, consumeError := colaEntrada.StartConsuming()
		if consumeError != 0 {
			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
		}

		colaSalida, err := middleware.CreateQueue("SalidaQuery3", middleware.ChannelOptionsDefault())
		if err != nil {
			panic(fmt.Errorf("CreateQueue(SalidaQuery3): %w", err))
		}

		worker := aggregator.Aggregator{}
		var result []packet.Packet
		for message := range *msgQueue {

			slog.Debug("Recibi mensaje")
			packet_reader := bytes.NewReader(message.Body)
			packet, _ := packet.DeserializePackage(packet_reader)

			println("Pkt enviado al aggregator:", packet.GetPayload())
			result = worker.Process(packet, "agregator3GlobalByMonthTPV")
			println("Resultado de aggregator:", result[0].GetPayload())
			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
			if len(result) != 0 {
				slog.Info("Obtuve EOF, mando todo empaquetado a la cola de sending")
				for _, pkt := range result {
					err := colaSalida.Send(pkt.Serialize())
					println(err)
				}
			}
		}
	}
}
