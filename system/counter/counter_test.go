package counter

import (
	"malasian_coffe/packet"
	"testing"
)

func TestCountByUserAndStore(t *testing.T) {
	// transaction_id,store_id,user_id
	input := "" +
		"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,1038745.0\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,3296.0\n" +
		"85f86fef-fddb-4eef-9dc3-1444553e6108,4,1038745.0\n" +
		"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,4,838764.0\n" +
		"51e44c8e-4812-4a15-a9f9-9a46b62424d6,7,3296.0\n"

	pkt := packet.Packet{}
	pkt = packet.ChangePayload(pkt, []string{input})[0]

	worker := &Counter{}
	outs := worker.Process(pkt)
	if len(outs) == 0 {
		t.Fatalf("Process devolvió 0 paquetes")
	}

	got := outs[0].GetPayload()

	// orden: store asc y luego user asc
	want := "" +
		"1038745.0,4,2\n" +
		"838764.0,4,1\n" +
		"3296.0,7,2\n"

	if got != want {
		t.Fatalf("salida inesperada.\nwant:\n%q\ngot:\n%q", want, got)
	}
}
