package filter_mapper

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"strconv"
	"strings"
	"time"
)

func yearCondition(time_s string) (bool, error) {
	if time_s == "" {
		return false, nil
	}

	layout := "2006-01-02 15:04:05" // Go's reference layout
	time_t, err := time.Parse(layout, time_s)
	if err != nil {
		return false, nil
	}
	return time_t.Year() >= 2024 && time_t.Year() <= 2025, nil
}

func hourCondition(time_s string) (bool, error) {
	if time_s == "" {
		return false, nil
	}

	layout := "2006-01-02 15:04:05" // Go's reference layout
	time_t, err := time.Parse(layout, time_s)
	if err != nil {
		return false, nil
	}

	return time_t.Hour() >= 6 && time_t.Hour() <= 23, nil
}

func amountCondition(amount_s string) (bool, error) {
	amount_f, err := strconv.ParseFloat(amount_s, 64)
	if err != nil {
		return false, nil
	}
	return amount_f >= 75.0, nil
}

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
	fmt.Println(user_id)
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

func transactionFilterQuery2a(year_condition bool, transaction_id string, quantity string, created_at string, buffer *strings.Builder) {
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

func transactionFilterQuery2b(year_condition bool, transaction_id string, subtotal string, created_at string, buffer *strings.Builder) {
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
			slog.Debug("Registro con menos de 6 columnas, dropeado", "line", line, "columns", len(data))
			continue
		}

		transaction_id := data[1]
		quantity       := data[2]
		subtotal       := data[4]
		created_at     := data[5]

		year_condition, err := yearCondition(created_at)
		if err != nil {
			slog.Error("Failed to parse year %w, skipping register", err)
			continue
		}

		transactionFilterQuery2a(year_condition, transaction_id, quantity, created_at, &final_query2a)

		transactionFilterQuery2b(year_condition, transaction_id, subtotal, created_at, &final_query2b)
	}
	return []string{final_query2a.String(), final_query2b.String()}
}

func mapStoreIdAndName(input string) []string {
	print("[FILTER STORES]:", input)
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 8 {
			panic("Invalid data format")
		}

		if data[0] == "" ||
			data[1] == "" {
			slog.Debug("Registro con campos de interes vacios, dropeado")
			continue
		}
		final += data[0] + "," + data[1] + "\n"
	}
	return []string{final, final}
}

func mapItemIdAndName(input string) []string {
	print("[FILTER MENU ITEMS]:", input)
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 6 {
			panic("Invalid data format")
		}

		if data[0] == "" ||
			data[1] == "" {
			slog.Debug("Registro con campos de interes vacios, dropeado")
			continue
		}
		final += data[0] + "," + data[1] + "\n"
	}
	return []string{final, final}
}

func filterFunctionQuery4UsersBirthdates(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 4 {
			panic("Invalid data format")
		}
		if data[0] == "" || data[2] == "" {
			slog.Debug("Registro con menos de 9 columnas, dropeado")
			continue
		}
		final += data[0] + "," + data[2] + "\n"
	}
	return final
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
			slog.Debug("Registro con menos de 9 columnas, dropeado")
			continue
		}

		transaction_id := data[0]
		store_id := data[1]
		user_id := data[4]
		amount_s := data[7]
		time_s := data[8]

		year_condition, err := yearCondition(time_s)
		if err != nil {
			slog.Error("Failed to parse year %s, skipping register", err)
			continue
		}
		hour_condition, err := hourCondition(time_s)
		if err != nil {
			slog.Error("Failed to parse hour %s, skipping register", err)
			continue
		}
		amount_condition, err := amountCondition(amount_s)
		if err != nil {
			slog.Error("Failed to parse amount %w, skipping register", err)
			continue
		}
		// Saco el punto de punto flotante del user id

		transactionFilterQuery4(year_condition, transaction_id, store_id, user_id, &final_query4)

		transactionFilterQuery3(year_condition, hour_condition, store_id, amount_s, time_s, &final_query3)

		transactionFilterQuery1(year_condition, hour_condition, amount_condition, transaction_id, amount_s, &final_query1)

	}
	return []string{final_query1.String(), final_query3.String(), final_query4.String()}
}

// =========================== TransactionFilter ==============================

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

// =========================== TransactionFilter ==============================

// ============================== StoreFilter =================================

type storeFilterMapper struct {
	colaEntradaStore *middleware.MessageMiddlewareQueue

	colaSalida3 *middleware.MessageMiddlewareQueue
	colaSalida4 *middleware.MessageMiddlewareQueue
}

func (sfm *storeFilterMapper) Build(rabbitAddr string) {
	colaEntradaStore := colas.InstanceQueue("DataStores", rabbitAddr)

	colaSalida3 := colas.InstanceQueue("FilteredStores3", rabbitAddr)
	colaSalida4 := colas.InstanceQueue("FilteredStores4", rabbitAddr)

	sfm.colaEntradaStore = colaEntradaStore

	sfm.colaSalida3 = colaSalida3
	sfm.colaSalida4 = colaSalida4
}

func (sfm *storeFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return sfm.colaEntradaStore
}

func (sfm *storeFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	mapped_stores := mapStoreIdAndName(input)
	newPayload := packet.ChangePayload(pkt, mapped_stores)
	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: sfm.colaSalida3,
		},
		{
			Packet:     newPayload[1],
			ColaSalida: sfm.colaSalida4,
		},
	}

	return outBoundMessage
}

// ============================== StoreFilter =================================

// =============================== UserFilter ==================================
type userFilterMapper struct {
	colaEntradaUsers *middleware.MessageMiddlewareQueue

	colaSalida4 *middleware.MessageMiddlewareQueue
}

func (ufm *userFilterMapper) Build(rabbitAddr string) {
	colaEntradaUsers := colas.InstanceQueue("DataUsers", rabbitAddr)

	colaSalida4 := colas.InstanceQueue("FilteredUsers4", rabbitAddr)

	ufm.colaEntradaUsers = colaEntradaUsers

	ufm.colaSalida4 = colaSalida4
}

func (ufm *userFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return ufm.colaEntradaUsers
}

func (ufm *userFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	filtered_users := []string{filterFunctionQuery4UsersBirthdates(input)}

	newPayload := packet.ChangePayload(pkt, filtered_users)
	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: ufm.colaSalida4,
		},
	}

	return outBoundMessage
}

// =============================== UserFilter ==================================

// =========================== MenuItemFilter ==============================

type menuItemFilterMapper struct {
	colaEntradaStore *middleware.MessageMiddlewareQueue

	colaSalida2a *middleware.MessageMiddlewareQueue
	colaSalida2b *middleware.MessageMiddlewareQueue
}

func (mifm *menuItemFilterMapper) Build(rabbitAddr string) {
	colaEntradaStore := colas.InstanceQueue("DataMenuItems", rabbitAddr)

	colaSalida2a := colas.InstanceQueue("FilteredMenuItems2a", rabbitAddr)
	colaSalida2b := colas.InstanceQueue("FilteredMenuItems2b", rabbitAddr)

	mifm.colaEntradaStore = colaEntradaStore

	mifm.colaSalida2a = colaSalida2a
	mifm.colaSalida2b = colaSalida2b

}

func (mifm *menuItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return mifm.colaEntradaStore
}

func (mifm *menuItemFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Ambas payloads iguales
	mapped_stores := mapItemIdAndName(input)
	newPayload := packet.ChangePayload(pkt, mapped_stores)
	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.colaSalida2a,
		},
		{
			Packet:     newPayload[0],
			ColaSalida: mifm.colaSalida2b,
		},
	}

	return outBoundMessage
}

// =========================== MenuItemFilter ==============================

// =========================== TransactionItemsFilter ==============================

type transactionItemFilterMapper struct {
	colaEntradaTransaction *middleware.MessageMiddlewareQueue

	colaSalida2a *middleware.MessageMiddlewareQueue
	colaSalida2b *middleware.MessageMiddlewareQueue
}

func (tifm *transactionItemFilterMapper) GetInput() *middleware.MessageMiddlewareQueue {
	return tifm.colaEntradaTransaction

}

func (tifm *transactionItemFilterMapper) Build(rabbitAddr string) {
	colaEntradaTransaction := colas.InstanceQueue("DataTransactionItems", rabbitAddr)

	colaSalida2a := colas.InstanceQueue("FilteredTransactionItems2a", rabbitAddr)
	colaSalida2b := colas.InstanceQueue("FilteredTransactionItems2b", rabbitAddr)

	tifm.colaEntradaTransaction = colaEntradaTransaction

	tifm.colaSalida2a = colaSalida2a
	tifm.colaSalida2b = colaSalida2b
}
func (tifm *transactionItemFilterMapper) Process(pkt packet.Packet) []packet.OutBoundMessage {
	input := pkt.GetPayload()

	// Vienen en este orden: final_query2a, final_query2b
	payloadResults := filterTransactionItems(input)

	newPayload     := packet.ChangePayload(pkt, payloadResults)

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload[0],
			ColaSalida: tifm.colaSalida2a,
		},
		{
			Packet:     newPayload[1],
			ColaSalida: tifm.colaSalida2b,
		},
	}

	return outBoundMessage
}

// =========================== TransactionItemsFilter ==============================

type FilterMapper interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string)

	// Devuelve referencia de la cola de la cual tiene que consumir
	GetInput() *middleware.MessageMiddlewareQueue

	// Funcio que hace el filtrado
	Process(pkt packet.Packet) []packet.OutBoundMessage
}

func FilterMapperBuilder(datasetName string, rabbitAddr string) FilterMapper {
	var filterMapper FilterMapper
	switch datasetName {
	case "transactions":
		filterMapper = &transactionFilterMapper{}
	case "stores":
		filterMapper = &storeFilterMapper{}
	case "users":
		filterMapper = &userFilterMapper{}
	case "menu_items":
		filterMapper = &menuItemFilterMapper{}
	case "transaction_items":
		filterMapper = &transactionItemFilterMapper{}
	default:
		panic(fmt.Sprintf("Unknown 'dataset' %s", datasetName))
	}

	filterMapper.Build(rabbitAddr)
	return filterMapper
}
