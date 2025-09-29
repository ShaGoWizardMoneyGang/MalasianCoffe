package joiner

import (
	"fmt"
	"malasian_coffe/packets/packet"
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
