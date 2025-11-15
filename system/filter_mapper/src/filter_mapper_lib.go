package filter_mapper

import (
	"fmt"
	"strconv"
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

type FilterMapper interface {
	// Funcion que inicializa las cosas que el filter necesita
	Build(rabbitAddr string, queueAmount map[string]uint64)

	// Funcio que hace el filtrado
	Process()
}

func FilterMapperBuilder(datasetName string, rabbitAddr string, queueAmounts map[string] uint64) FilterMapper {
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

	filterMapper.Build(rabbitAddr, queueAmounts)
	return filterMapper
}
