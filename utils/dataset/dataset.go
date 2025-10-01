package dataset

import (
	"fmt"
)


// Helper function que traduce el nombre del directorio con datos a su ID corresponditente.
// ADVERTENCIA: Programacion avanzada.
func DatasetToID(directory_name string) (uint64, error) {
	var id uint64
	switch directory_name {
	case "menu_items":
		id = 0
	case "stores":
		id = 1
	case "transaction_items":
		id = 2
	case "transactions":
		id = 3
	case "users":
		id = 4
	default:
		return 0, fmt.Errorf("Unrecognized directory_name %s", directory_name)
	}
	return id, nil
}

// Helper function que traduce el nombre del directorio con datos a su ID corresponditente.
// ADVERTENCIA: Programacion avanzada (posta).
func IDtoDataset(directory_id uint64) (string, error) {
	var directory_name string
	switch directory_id {
	case 0:
		directory_name = "menu_items"
	case 1:
		directory_name = "stores"
	case 2:
		directory_name = "transaction_items"
	case 3:
		directory_name = "transactions"
	case 4:
		directory_name = "users"
	default:
		return "", fmt.Errorf("Unrecognized directory id %d", directory_id)
	}
	return directory_name, nil
}
