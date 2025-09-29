package main

import (
	"bytes"
	"fmt"
	"malasian_coffe/packets/packet"
	filter_mapper "malasian_coffe/system/filter_mapper/src"
	"malasian_coffe/system/middleware"
)

func main() {
	// Hay que ponerle el arg del rabbit
	colaEntrada, err := middleware.CreateQueue("DataQuery1", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(DataQuery1): %w", err))
	}
	// defer colaEntrada.Close()

	// arranca a consumir
	msgQueue, consumeError := colaEntrada.StartConsuming()
	if consumeError != 0 {
		panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
	}
	// defer colaEntrada.StopConsuming()

	worker := filter_mapper.FilterMapper{}
	var result []packet.Packet

	// msgQueue es **<-chan amqp.Delivery HORRIBLE
	for message := range **msgQueue {
		packetReader := bytes.NewReader(message.Body)
		pkt, _ := packet.DeserializePackage(packetReader)

		fmt.Printf("paquete recibido: %+v\n", pkt)
		paqueteSalida := worker.Process(pkt, "query1YearAndAmount")
		result = append(result, paqueteSalida)
		fmt.Printf("paquete procesado: %+v\n", paqueteSalida)
		if pkt.IsEOF() {
			break
		}
	}

	// cola de salida, envio
	colaSalida, err := middleware.CreateQueue("FilterMapper1YearAndAmount", middleware.ChannelOptionsDefault())
	if err != nil {
		panic(fmt.Errorf("CreateQueue(FilterMapper1YearAndAmount): %w", err))
	}
	// defer colaSalida.Close()

	for _, pkt := range result {
		_ = colaSalida.Send(pkt.Serialize())
	}
}
