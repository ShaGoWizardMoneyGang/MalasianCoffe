package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/network"
	"os"
	"strings"
)

type joinerOptions func(*Joiner, string) string

type Joiner struct {
	Function  joinerOptions
	Stores    map[string]string // aca me voy a guardar en memoria las tablas chica
	MenuItems map[string]string
}

// Recibe year_half_created_at, store_id, tpv
// joinea con stores.csv cargado en memoria con store_id, store_name
// y me devuelve year_half_created_at, store_name, tpv
func (j *Joiner) joinerFunctionQuery3(input string) string {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]
	var b strings.Builder
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		semester, storeID, tpv := cols[0], cols[1], cols[2]
		storeName := j.Stores[storeID]
		fmt.Fprintf(&b, "%s,%s,%s\n", semester, storeName, tpv)
	}
	return b.String()
}

// Recibe year_month_created_at, item_id, quantity
// joinea con stores.csv cargado en memoria con item_id, item_name
// y me devuelve year_month_created_at, item_name, quantity
func (j *Joiner) joinerFunctionQuery2Quantity(input string) string {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]
	var b strings.Builder
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		month, itemID, quantity := cols[0], cols[1], cols[2]
		itemName := j.MenuItems[itemID]
		fmt.Fprintf(&b, "%s,%s,%s\n", month, itemName, quantity)
	}
	return b.String()
}

// Recibe year_month_created_at, item_id, subtotal
// joinea con menu_items.csv cargado en memoria con item_id, item_name
// y me devuelve year_month_created_at, item_name, subtotal
func (j *Joiner) joinerFunctionQuery2Subtotal(input string) string {
	lines := strings.Split(input, "\n")
	lines = lines[:len(lines)-1]
	var b strings.Builder
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		month, itemID, quantity := cols[0], cols[1], cols[2]
		storeName := j.MenuItems[itemID]
		fmt.Fprintf(&b, "%s,%s,%s\n", month, storeName, quantity)
	}
	return b.String()
}

func (c *Joiner) Process(pkt packet.Packet, function string) []packet.Packet {
	input := pkt.GetPayload()
	function_name := strings.ToLower(function)

	var output string
	switch function_name {
	case "query3":
		output = c.joinerFunctionQuery3(input)
	case "query2quantity":
		output = c.joinerFunctionQuery2Quantity(input)
	case "query2subtotal":
		output = c.joinerFunctionQuery2Subtotal(input)
	default:
		panic(fmt.Sprintf("Unknwon function %s", function))
	}

	outputs := []string{output}
	new_packet := packet.ChangePayload(pkt, outputs)

	return new_packet
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
		colaEntradaStores, err := middleware.CreateQueue("FilteredStores", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
		if err != nil {
			panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
		}
		msgQueue, consumeError := colaEntradaStores.StartConsuming()
		if consumeError != 0 {
			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
		}
		worker := Joiner{}
		worker.Stores = make(map[string]string) //genero el map vacío

		for message := range *msgQueue { //idealmente esto sería solo una vez para cargar las 10 stores en memoria
			println("[JOINER QUERY3] Leyendo stores para memoria")
			// slog.Debug("[Joiner Q3] Leyendo stores para memoria")
			packet_reader := bytes.NewReader(message.Body)
			packet, _ := packet.DeserializePackage(packet_reader)
			lines := strings.Split(packet.GetPayload(), "\n") //leo el paquete
			lines = lines[:len(lines)-1]

			for _, r := range lines { //por cada linea del paquete
				cols := strings.Split(r, ",")
				println("[JOINER QUERY3] Store ", cols[1], " para memoria")
				if len(cols) < 2 {
					panic("No hay 2 columnas como se esperaba") //store_id, store_name
				}
				worker.Stores[cols[0]] = cols[1] //lo guardo en memoria
			}
			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
		}
		println("[JOINER QUERY3] Leyendo global aggregations")
		colaEntradaTransactions, err := middleware.CreateQueue("GlobalAggregation3", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbitAddr)})
		if err != nil {
			panic(fmt.Errorf("CreateQueue(COLA DE ENTRADA): %w", err))
		}
		msgQueueTransactions, consumeErrorT := colaEntradaTransactions.StartConsuming()
		if consumeErrorT != 0 {
			panic(fmt.Errorf("StartConsuming failed with code %d", consumeError))
		}
		for message := range *msgQueueTransactions { //acá debería haber solo 1 paquete del global aggregator
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			paquetesSalida := worker.Process(pkt, "query3")
			fmt.Printf("PAQUETES SALIDA %v\n", paquetesSalida)
			for _, pkt := range paquetesSalida {
				slog.Info("[Joiner Q3] Mando packet joineado a siguiente cola")
				colaSalida, err := middleware.CreateQueue("SalidaQuery3", middleware.ChannelOptionsDefault())
				if err != nil {
					panic(fmt.Errorf("CreateQueue(SalidaQuery3): %w", err))
				}
				_ = colaSalida.Send(pkt.Serialize())
			}

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}
		}
	}

}
