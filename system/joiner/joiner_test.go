package joiner

import (
	"malasian_coffe/packet"
	"testing"
)

func TestJoinByStoreNameQuery2(t *testing.T) {
	j := &Joiner{
		Function: joinerFunctionQuery2,
		Stores: map[string]string{
			"1":  "G Coffee @ USJ 89q",
			"2":  "G Coffee @ Kondominium Putra",
			"3":  "G Coffee @ USJ 57W",
			"4":  "G Coffee @ Kampung Changkat",
			"5":  "G Coffee @ Seksyen 21",
			"6":  "G Coffee @ Alam Tun Hussein Onn",
			"7":  "G Coffee @ Damansara Saujana",
			"8":  "G Coffee @ Bandar Seri Mulia",
			"9":  "G Coffee @ PJS8",
			"10": "G Coffee @ Taman Damansara",
		},
	}

	transactions := "2025-H1,1,123\n" +
		"2025-H2,2,99\n" +
		"2025-H1,6,10\n"

	paqueteSalida := j.Process(packet.Packet{Payload: []byte(transactions)})
	salida := string(paqueteSalida.Payload)

	esperado := "" +
		"2025-H1,G Coffee @ USJ 89q,123\n" +
		"2025-H2,G Coffee @ Kondominium Putra,99\n" +
		"2025-H1,G Coffee @ Alam Tun Hussein Onn,10\n"

	if esperado != salida {
		panic("LO que salio no coincide con lo esperado")
	}
}
