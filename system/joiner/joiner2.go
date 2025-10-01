package main

import (
	"bytes"
	"fmt"
	"os"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
)

type JoinerQuery3 struct {
	// Tenemos una go routine por cada session
	sessions map[string] (chan packet.Packet)

	canalSalida chan packet.Packet

	colaSalidaQuery3  *middleware.MessageMiddlewareQueue
}

func (jq3 *JoinerQuery3) passPacketToJoiner(pkt packet.Packet) {
	sessionID := pkt.GetSessionID()
	channel, exists := jq3.sessions[sessionID]


	// Nos creamos una rutina por cada session. Nosotros le enviamos los
	// paquetes correspondientes a cada rutina
	if !exists {
		// Joiner
		assigned_channel := make(chan packet.Packet)
		go func() {
			for {
				pkt :=  <- assigned_channel
				// TODO aca hacer el join
				if pkt.IsEOF() {
					break
				}
			}
		}()
		// No hace falta un mutex porque este diccionario se accede de forma
		// secuencial
		jq3.sessions[sessionID] = assigned_channel

		// Ahora que lo tenemos, sobre escribimos el valor basura que obtuvimos antes.
		channel = assigned_channel
	}

	channel <- pkt
}

func (jq3 *JoinerQuery3) Build(rabbitAddr string) {
	// SessionID -> channel
	sessionHandler           := make(map[string](chan packet.Packet))
	canalSalida              := make(chan packet.Packet)

	colaSalidaQuery3  := colas.InstanceQueue("SalidaQuery3", rabbitAddr)

	jq3.sessions = sessionHandler

	jq3.colaSalidaQuery3 = colaSalidaQuery3

	jq3.canalSalida = canalSalida
}

func (jq3 *JoinerQuery3) send(pkt packet.Packet) {
	// SessionID -> channel
	jq3.colaSalidaQuery3.Send(pkt.Serialize())
}

func (jq3 *JoinerQuery3) Process(rabbitAddr string) {
	storeListener            := make(chan packet.Packet)
	aggregatorGlobalListener := make(chan packet.Packet)


	// Stores
	go func() {
		colasEntrada := colas.InstanceQueue("FilteredStores3", rabbitAddr)

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			storeListener <- pkt
		}
	}()

	// Aggregated Filtered Transactions
	go func() {
		// TODO: Maybe guardar en el struct
		colasEntrada := colas.InstanceQueue("GlobalAggregation3", rabbitAddr)

		messages := colas.ConsumeInput(colasEntrada)

		for message := range *messages {
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			aggregatorGlobalListener <- pkt
		}
	}()

	for {
		select {
		case storePacket := <-storeListener:
			jq3.passPacketToJoiner(storePacket)
		case aggregatedPacket := <-aggregatorGlobalListener:
			jq3.passPacketToJoiner(aggregatedPacket)
		case packetJoineado := <-jq3.canalSalida:
			jq3.send(packetJoineado)
		}
	}
}

func main() {
	joinFunction := os.Args[2]
	if len(joinFunction) == 0 {
		panic("No join function provided")
	}
	rabbitAddr := os.Args[1]

	switch joinFunction {
	case "Query3":
		println("[JOINER QUERY3]")
		joinQuery3()
	}
}
