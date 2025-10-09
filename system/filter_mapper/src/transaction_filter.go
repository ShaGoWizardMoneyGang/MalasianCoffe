package filter_mapper

import (
	"fmt"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strings"
)

func transactionFilterQuery4(year_condition bool, transaction_id string, store_id string, user_id string, buffer *strings.Builder) {
	if transaction_id == "" ||
		store_id == "" ||
		user_id == "" {
		return
	}

	if !year_condition {
		return
	}

	// NOTE: Descartamos el .0 anadido que tienen por algun motivo
	user_id_int := user_id[:len(user_id)-2]

	new_line := transaction_id + "," + store_id + "," + user_id_int + "\n"

	buffer.WriteString(new_line)
}

func transactionFilterQuery3(year_condition bool, hour_condition bool, store_id string, amount_s string, time_s string, buffer *strings.Builder) {
	if store_id == "" ||
		amount_s == "" ||
		time_s == "" {
		return
	}

	if !year_condition || !hour_condition {
		return
	}

	new_line := store_id + "," + amount_s + "," + time_s + "\n"

	buffer.WriteString(new_line)
}

func transactionFilterQuery1(year_condition bool, hour_condition bool, amount_condition bool, transaction_id string, amount_s string, buffer *strings.Builder) {
	if transaction_id == "" ||
		amount_s == "" {
		return
	}

	if !year_condition || !hour_condition || !amount_condition {
		return
	}

	new_line := transaction_id + "," + amount_s + "\n"

	buffer.WriteString(new_line)
}

func filterTransactions(input string) []string {

	lines := strings.Split(input, "\n")

	var final_query1 strings.Builder
	var final_query3 strings.Builder
	var final_query4 strings.Builder
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		data := strings.Split(line, ",")
		if len(data) < 9 {
			bitacora.Debug("Registro con menos de 9 columnas, dropeado")
			continue
		}

		transaction_id := data[0]
		store_id := data[1]
		user_id := data[4]
		amount_s := data[7]
		time_s := data[8]

		year_condition, err := yearCondition(time_s)
		if err != nil {
			bitacora.Error(fmt.Sprintf("Failed to parse year %s, skipping register", err))
			continue
		}
		hour_condition, err := hourCondition(time_s)
		if err != nil {
			bitacora.Error(fmt.Sprintf("Failed to parse hour %s, skipping register", err))
			continue
		}
		amount_condition, err := amountCondition(amount_s)
		if err != nil {
			bitacora.Error(fmt.Sprintf("Failed to parse amount %s, skipping register", err))
			continue
		}
		// Saco el punto de punto flotante del user id

		transactionFilterQuery4(year_condition, transaction_id, store_id, user_id, &final_query4)

		transactionFilterQuery3(year_condition, hour_condition, store_id, amount_s, time_s, &final_query3)

		transactionFilterQuery1(year_condition, hour_condition, amount_condition, transaction_id, amount_s, &final_query1)

	}
	return []string{final_query1.String(), final_query3.String(), final_query4.String()}
}

type transactionFilterMapper struct {
	colaEntradaTransaction *middleware.MessageMiddlewareQueue

	colaSalida1 *middleware.MessageMiddlewareQueue
	colaSalida3 *middleware.MessageMiddlewareQueue
	colaSalida4 *middleware.MessageMiddlewareQueue
}

func (tfm *transactionFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return tfm.colaEntradaTransaction

}

func (tfm *transactionFilterMapper) Build(rabbitAddr string) {
	colaEntradaTransaction := colas.InstanceQueue("DataTransactions", rabbitAddr)

	colaSalida1 := colas.InstanceQueue("FilteredTransactions1", rabbitAddr)
	colaSalida3 := colas.InstanceQueue("FilteredTransactions3", rabbitAddr)
	colaSalida4 := colas.InstanceQueue("FilteredTransactions4", rabbitAddr)

	tfm.colaEntradaTransaction = colaEntradaTransaction

	tfm.colaSalida1 = colaSalida1
	tfm.colaSalida3 = colaSalida3
	tfm.colaSalida4 = colaSalida4
}
func (tfm *transactionFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Vienen en este orden: final_query1, final_query3, final_query4
	payloadResults := filterTransactions(input)

	newPayload := packet.ChangePayload(pkt, payloadResults)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: tfm.colaSalida1,
		},
		{
			Packet:     newPayload[1],
			ColaSalida: tfm.colaSalida3,
		},
		{
			Packet:     newPayload[2],
			ColaSalida: tfm.colaSalida4,
		},
	}

	return outBoundMessage
}
