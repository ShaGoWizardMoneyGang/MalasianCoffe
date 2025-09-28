package counter

import (
	"fmt"
	"malasian_coffe/packet"
	"sort"
	"strings"
)

// Recibe transaction_id, store_id, user_id
// Devuelve user_id,store_id,cantidad por línea, ordenado por store_id y luego user_id para mantenerlo determinístico
func (c *Counter) countFunctionQuery4(input string) string {
	rows := strings.Split(input, "\n")
	rows = rows[:len(rows)-1] // El split me genera 1 linea de mas vacia por el ultimo /n, la ignoro

	type key struct {
		store string
		user  string
	}
	// tengo un map donde voy contando las apariciones por store y user
	counts := map[key]int{}

	for _, r := range rows {

		cols := strings.Split(r, ",") // este comportamiento podria modularizarlo para todos
		if len(cols) < 3 {
			panic("Invalid data format")
		}

		storeID := cols[1]
		userID := cols[2]
		counts[key{store: storeID, user: userID}]++
	}

	// ordeno, esto podria modularizarlo???
	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].store == keys[j].store {
			return keys[i].user < keys[j].user
		}
		return keys[i].store < keys[j].store
	})

	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s,%s,%d\n", k.user, k.store, counts[k])
	}
	return b.String()
}

type Counter struct {
}

func (c *Counter) Process(pkt packet.Packet) []packet.Packet {
	// unica funcion de counter, va directo
	input := pkt.GetPayload()
	output := c.countFunctionQuery4(input)
	outputs := []string{output}
	newPacket := packet.ChangePayload(pkt, outputs)

	return newPacket
}
