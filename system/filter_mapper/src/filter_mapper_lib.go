package filter_mapper

import (
	"fmt"
	"log/slog"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"math"
	"strconv"
	"strings"
	"time"
)

type filterMapperOptions func(string) string

func filterByYearCommon(input string) string {
	input_lines := strings.Split(input, "\n")
	final := ""
	for _, line := range input_lines {
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		if yearCondition(data) {
			final += line + "\n"
		}
	}
	return final
}

func yearCondition(data []string) bool {
	layout := "2006-01-02 15:04:05" // Go's reference layout
	t, _ := time.Parse(layout, data[8])
	return t.Year() >= 2024 && t.Year() <= 2025
}

func filterFunctionQuery1(input string) string {
	lines := strings.Split(input, "\n")

	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10

		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[8])

		if yearCondition(data) && amount >= 75.0 && t.Hour() >= 6 && t.Hour() <= 23 {
			final += data[0] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "\n"
		}
	}
	return final
}

func filterFunctionQuery2a(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 6 {
			slog.Debug("Registro con menos de 6 columnas, dropeado", "line", line, "columns", len(data))
			continue
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, err := time.Parse(layout, data[5])
		if err != nil {
			slog.Debug("Error parsing timestamp, dropeado", "timestamp", data[5], "error", err)
			continue
		}
		if t.Year() >= 2024 && t.Year() <= 2025 {
			final += data[1] + "," + data[2] + "," + data[5] + "\n"
		}
	}
	return final
}

func filterFunctionQuery2b(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 6 {
			slog.Debug("Registro con menos de 6 columnas, dropeado", "line", line, "columns", len(data))
			continue
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, err := time.Parse(layout, data[5])
		if err != nil {
			slog.Debug("Error parsing timestamp, dropeado", "timestamp", data[5], "error", err)
			continue
		}
		if t.Year() >= 2024 && t.Year() <= 2025 {
			final += data[1] + "," + data[4] + "," + data[5] + "\n"
		}
	}
	return final
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

func filterFunctionQuery3Transactions(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[8])
		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10
		if yearCondition(data) && t.Hour() >= 6 && t.Hour() <= 23 {
			final += data[1] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "," + data[8] + "\n"
		}
	}
	return final
}

func filterFunctionQuery4Transactions(input string) string {
	lines := strings.Split(input, "\n")
	final := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			panic("Invalid data format")
		}
		if yearCondition(data) {
			final += data[0] + "," + data[1] + "," + data[4] + "\n"
		}
	}
	return final
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
	final_query1 := ""
	final_query3 := ""
	final_query4 := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 9 {
			slog.Debug("Registro con menos de 9 columnas, dropeado")
			continue
		}

		// TODO(fabri): Revisar como handlear el dropeo, tal vez tiene que ser mas quirurquico.
		if data[0] == "" ||
			data[1] == "" ||
			data[4] == "" ||
			data[7] == "" ||
			data[8] == "" {
			slog.Debug("Registro con campos de interes vacios, dropeado")
			continue
		}

		amount, _ := strconv.ParseFloat(data[7], 64)
		amount = math.Round(amount*10) / 10
		layout := "2006-01-02 15:04:05" // Go's reference layout
		t, _ := time.Parse(layout, data[8])

		if yearCondition(data) {
			id_float := data[4]
			id_int   := id_float[:len(id_float)-2]
			final_query4 += data[0] + "," + data[1] + "," + id_int + "\n" //mapeo query 4
			if t.Hour() >= 6 && t.Hour() <= 23 {
				final_query3 += data[1] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "," + data[8] + "\n" //mapeo query 3
				if amount >= 75.0 {
					final_query1 += data[0] + "," + strconv.FormatFloat(amount, 'f', 1, 64) + "\n"
				}
			}
		}
	}
	return []string{final_query1, final_query3, final_query4}
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

	payloadResults2a := filterFunctionQuery2a(input)
	payloadResults2b := filterFunctionQuery2b(input)

	newPayload2a := packet.ChangePayload(pkt, []string{payloadResults2a})
	newPayload2b := packet.ChangePayload(pkt, []string{payloadResults2b})

	outBoundMessage := []packet.OutBoundMessage{
		{
			Packet:     newPayload2a[0],
			ColaSalida: tifm.colaSalida2a,
		},
		{
			Packet:     newPayload2b[0],
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
