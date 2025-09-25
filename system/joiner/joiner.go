package joiner

import (
	"fmt"
	"malasian_coffe/packet"
	"strings"
)

type joinerOptions func(*Joiner, string) string

var (
	// Recibe year_half_created_at, store_id, tpv
	// joinea con stores.csv cargado en memoria con store_id, store_name
	// y me devuelve year_half_created_at, store_name, tpv
	joinerFunctionQuery2 joinerOptions = func(j *Joiner, input string) string {
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
)

type Joiner struct {
	Function joinerOptions
	Stores   map[string]string // aca me voy a guardar en memoria las tablas chicas, por
	// ahora se llama stores
}

func (c *Joiner) Process(pkt packet.Packet) packet.Packet {
	returned := packet.Packet{Payload: []byte("")}
	if c.Function != nil {
		res := c.Function(c, string(pkt.Payload))
		returned.Payload = []byte(res)
	}
	return returned
}
