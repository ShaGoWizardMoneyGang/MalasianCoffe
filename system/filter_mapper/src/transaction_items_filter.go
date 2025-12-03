package filter_mapper

import (
	"fmt"
	"log/slog"
	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
	"strings"
)

func transactionItemsFilterQuery2a(year_condition bool, transaction_id string, quantity string, created_at string, buffer *strings.Builder) {
	if transaction_id == "" ||
		quantity == "" ||
		created_at == "" {
		return
	}

	if !year_condition {
		return
	}

	new_line_2a := transaction_id + "," + quantity + "," + created_at + "\n"
	buffer.WriteString(new_line_2a)
}

func transactionItemsFilterQuery2b(year_condition bool, transaction_id string, subtotal string, created_at string, buffer *strings.Builder) {
	if transaction_id == "" ||
		subtotal == "" ||
		created_at == "" {
		return
	}

	if !year_condition {
		return
	}

	new_line_2b := transaction_id + "," + subtotal + "," + created_at + "\n"
	buffer.WriteString(new_line_2b)
}

// 0: transaction_id
// 1: item_id
// 2: quantity
// 3: unitPrice
// 4: subtotal
// 5: created at
func filterTransactionItems(input string) []string {
	lines := strings.Split(input, "\n")

	var final_query2a strings.Builder
	var final_query2b strings.Builder
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		data := strings.Split(line, ",")
		if len(data) < 6 {
			bitacora.Debug(fmt.Sprintf("Registro con menos de 6 columnas"))
			continue
		}

		transaction_id := data[1]
		quantity := data[2]
		subtotal := data[4]
		created_at := data[5]

		year_condition, err := yearCondition(created_at)
		if err != nil {
			bitacora.Error(fmt.Sprintf("Failed to parse year %s, skipping register", err))
			continue
		}

		transactionItemsFilterQuery2a(year_condition, transaction_id, quantity, created_at, &final_query2a)

		transactionItemsFilterQuery2b(year_condition, transaction_id, subtotal, created_at, &final_query2b)
	}
	return []string{final_query2a.String(), final_query2b.String()}
}

// =========================== TransactionItemsFilter ==============================

type transactionItemFilterMapper struct {
	packet_channel chan colas.PacketMessage

	colaEntradaTransaction *middleware.MessageMiddlewareQueue

	colaSalida2a *middleware.MessageMiddlewareQueue
	colaSalida2b *middleware.MessageMiddlewareQueue
}

func (tifm *transactionItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return tifm.colaEntradaTransaction

}

func (tifm *transactionItemFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	tifm.packet_channel = make(chan colas.PacketMessage)

	tifm.colaEntradaTransaction = colas.InstanceQueue("DataTransactionItems", rabbitAddr)

	tifm.colaSalida2a = colas.InstanceQueue("FilteredTransactionItems2a", rabbitAddr)
	tifm.colaSalida2b = colas.InstanceQueue("FilteredTransactionItems2b", rabbitAddr)
}

func (tifm *transactionItemFilterMapper) Process() {
	slog.Info("Arranca procesamiento de store filter mapper")

	go colas.InputQueue(tifm.colaEntradaTransaction, tifm.packet_channel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-tifm.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()

			// Vienen en este orden: final_query2a, final_query2b
			payloadResults := filterTransactionItems(input)

			newPayload := packet.ChangePayload(pkt, payloadResults)

			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: tifm.colaSalida2a,
				},
				{
					Packet:     newPayload[1],
					ColaSalida: tifm.colaSalida2b,
				},
			}
			for _, outbound := range outBoundMessages {
				cola := outbound.ColaSalida
				packet := outbound.Packet
				cola.Send(packet)
			}
			message.Ack(false)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			watchdog.Pong(IP)
		}

	}
}

// =========================== TransactionItemsFilter ==============================
