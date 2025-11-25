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
	packet_channel chan colas.PacketMessage

	colaEntradaTransaction *middleware.MessageMiddlewareQueue

	exchangeSalida1 *middleware.MessageMiddlewareExchange
	colaSalida3     *middleware.MessageMiddlewareQueue
	colaSalida4     *middleware.MessageMiddlewareQueue
}

func (tfm *transactionFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return tfm.colaEntradaTransaction

}

func (tfm *transactionFilterMapper) Build(rabbitAddr string, queueAmount map[string]uint64) {
	tfm.packet_channel = make(chan colas.PacketMessage)

	tfm.colaEntradaTransaction = colas.InstanceQueue("DataTransactions", rabbitAddr)

	tfm.exchangeSalida1 = colas.InstanceExchange("FilteredTransactions1", rabbitAddr, queueAmount["queue1"])

	tfm.colaSalida3 = colas.InstanceQueue("FilteredTransactions3", rabbitAddr)
	tfm.colaSalida4 = colas.InstanceQueue("FilteredTransactions4", rabbitAddr)
}
func (tfm *transactionFilterMapper) Process() {
	slog.Info("Arranca procesamiento de store filter mapper")

	go colas.InputQueue(tfm.colaEntradaTransaction, tfm.packet_channel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case pkt_message := <-tfm.packet_channel:
			pkt := pkt_message.Packet
			message := pkt_message.Message

			input := pkt.GetPayload()

			// Vienen en este orden: final_query1, final_query3, final_query4
			payloadResults := filterTransactions(input)

			newPayload := packet.ChangePayload(pkt, payloadResults)

			outBoundMessages := []colas.OutBoundMessage{
				{
					Packet:     newPayload[0],
					ColaSalida: tfm.exchangeSalida1,
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

			for _, outbound := range outBoundMessages {
				cola := outbound.ColaSalida
				packet := outbound.Packet
				cola.Send(packet)
			}

			message.Ack(false)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Filter Transactions received healthcheck ping from", IP)
			watchdog.Pong(IP)
		}
	}
}
