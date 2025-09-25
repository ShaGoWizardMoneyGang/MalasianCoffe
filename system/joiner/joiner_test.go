package joiner

import (
	"malasian_coffe/packet"
	"testing"
)

func TestJoinByStoreNameQuery3(t *testing.T) {
	j := &Joiner{
		Function: joinerFunctionQuery3,
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

func TestJoinByItemNameQuery2Quantity(t *testing.T) {
	j := &Joiner{
		Function: joinerFunctionQuery2Quantity,
		MenuItems: map[string]string{
			"1": "Espresso",
			"2": "Americano",
			"3": "Latte",
			"4": "Cappuccino",
			"5": "Flat White",
			"6": "Mocha",
			"7": "Hot Chocolate",
			"8": "Matcha Latte",
		},
	}

	transaction_items := "2024-12,2,150439\n" +
		"2025-01,3,150392\n" +
		"2025-03,6,155677\n"

	paqueteSalida := j.Process(packet.Packet{Payload: []byte(transaction_items)})
	salida := string(paqueteSalida.Payload)

	esperado := "" +
		"2024-12,Americano,150439\n" +
		"2025-01,Latte,150392\n" +
		"2025-03,Mocha,155677\n"

	if esperado != salida {
		panic("LO que salio no coincide con lo esperado")
	}
}

func TestJoinByItemNameQuery2Subtotal(t *testing.T) {
	j := &Joiner{
		Function: joinerFunctionQuery2Subtotal,
		MenuItems: map[string]string{
			"1": "Espresso",
			"2": "Americano",
			"3": "Latte",
			"4": "Cappuccino",
			"5": "Flat White",
			"6": "Mocha",
			"7": "Hot Chocolate",
			"8": "Matcha Latte",
		},
	}

	transaction_items := "2024-12,2,2990050.0\n" +
		"2025-01,3,3104830\n" +
		"2025-03,6,3002390.0\n"

	paqueteSalida := j.Process(packet.Packet{Payload: []byte(transaction_items)})
	salida := string(paqueteSalida.Payload)

	esperado := "" +
		"2024-12,Americano,2990050.0\n" +
		"2025-01,Latte,3104830\n" +
		"2025-03,Mocha,3002390.0\n"

	if esperado != salida {
		panic("LO que salio no coincide con lo esperado")
	}
}
