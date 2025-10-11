package joiner

import (
	"malasian_coffe/packets/packet"
	"strings"
)

// Devuelve true cuando quedan recibio todos los paquetes, sino false
func addStoreToMap(storePkt packet.Packet, storeMap map[string]string) bool {
	stores := storePkt.GetPayload()
	lines := strings.Split(stores, "\n")
	lines = lines[:len(lines)-1]
	for _, line := range lines {
		// store_id , store_name
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeMap[store_id] = store_name
	}

	// TODO: verificar paquetes fuera de orden. En teoria se deberia poder
	// aislar en esta funcion o packet receiver en el directorio de packets
	// Ver: https://github.com/ShaGoWizardMoneyGang/MalasianCoffe/issues/46
	if storePkt.IsEOF() {
		// Me llegaron todos
		return true
	} else {
		return false
	}
}

func createStoreMap(storeReceiver packet.PacketReceiver) map[string]string {
	stores := storeReceiver.GetPayload()
	lines := strings.Split(stores, "\n")
	lines = lines[:len(lines)-1]

	// Le damos un tamano inicial de lines porque deberia tener un tamano igual
	// al de la cantidad de lineas. Ademas, ya pre-alocamos la memoria.
	storeID2Name := make(map[string]string, len(lines))
	for _, line := range lines {
		// store_id , store_name
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeID2Name[store_id] = store_name
	}

	return storeID2Name
}

func createMenuItemMap(menuItemReceiver packet.PacketReceiver) map[string]string {
	menuItemPkt := menuItemReceiver.GetPayload()
	lines := strings.Split(menuItemPkt, "\n")
	lines = lines[:len(lines)-1]
	itemID2Name := make(map[string]string, len(lines))

	for _, line := range lines {
		// item_id, item_name
		cols := strings.Split(line, ",")
		item_id, item_name := cols[0], cols[1]
		itemID2Name[item_id] = item_name
	}

	return itemID2Name
}

