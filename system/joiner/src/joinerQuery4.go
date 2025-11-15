package joiner

import (
	"fmt"
	"strconv"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/system/watchdog"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery4 struct {
	inputChannel chan colas.PacketMessage

	outputChannel chan packet.Packet

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaUsersInput    *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery4 *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func createUserMap(userReceiver packet_receiver.PacketReceiver) map[string]string {
	storePkt := userReceiver.GetPayload()
	lines := strings.Split(storePkt, "\n")
	lines = lines[:len(lines)-1]

	// Le damos un tamano inicial de lines porque deberia tener un tamano igual
	// al de la cantidad de lineas. Ademas, ya pre-alocamos la memoria.
	storeID2Name := make(map[string]string, len(lines))

	for _, line := range lines {
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeID2Name[store_id] = store_name
	}

	return storeID2Name
}

func joinQuery4(inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	storeReceiver := packet_receiver.NewPacketReceiver("Store")

	userReceiver := packet_receiver.NewPacketReceiver("User")

	// Aca me guardo todos los packets de transactions que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	transactionReceiver := packet_receiver.NewPacketReceiver("Transactions")

	// Resultado final
	var joinedTransactions strings.Builder

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "stores" {
			storeReceiver.ReceivePacket(pktMsg)
		} else if dataset_name == "users" {
			userReceiver.ReceivePacket(pktMsg)
		} else if dataset_name == "transactions" {
			transactionReceiver.ReceivePacket(pktMsg)

		} else {
			bitacora.Error(fmt.Sprintf("JoinerQuery4 received packet from dataset that was not expecting: %s", dataset_name))
		}

		// No joineamos hasta tenerlo todo.
		if storeReceiver.ReceivedAll() && userReceiver.ReceivedAll() && transactionReceiver.ReceivedAll() {
			// jq4.logger.Info("Comienza proceso de join")
			joinerFunctionQuery4(storeReceiver, userReceiver, transactionReceiver, &joinedTransactions)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"users", "stores", "transactions"}, []string{joinedTransactions.String()})

	// Liberamos
	joinedTransactions.Reset()

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}
}

func (jq4 *joinerQuery4) Build(rabbitAddr string, routingKey string) {
	jq4.inputChannel = make(chan colas.PacketMessage)

	jq4.outputChannel = make(chan packet.Packet)

	jq4.colaStoresInput = colas.InstanceQueueRouted("FilteredStores4", rabbitAddr, routingKey)

	jq4.colaUsersInput = colas.InstanceQueueRouted("FilteredUsers4", rabbitAddr, routingKey)

	jq4.colaAggTransInput = colas.InstanceQueueRouted("GlobalAggregation4", rabbitAddr, routingKey)

	jq4.colaSalidaQuery4 = colas.InstanceQueue("SalidaQuery4", rabbitAddr)

	jq4.sessionHandler = sessionhandler.NewSessionHandler(joinQuery4, jq4.outputChannel)

}

func (jq4 *joinerQuery4) Process() {
	go colas.InputQueue(jq4.colaStoresInput, jq4.inputChannel)

	go colas.InputQueue(jq4.colaUsersInput, jq4.inputChannel)

	go colas.InputQueue(jq4.colaAggTransInput, jq4.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	go watchdog.Listen()

	for {
		select {
		case inputPacket := <-jq4.inputChannel:
			jq4.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq4.outputChannel:
			jq4.colaSalidaQuery4.Send(packetJoineado)
		}
	}
}

func joinerFunctionQuery4(storeReceiver packet_receiver.PacketReceiver, userReceiver packet_receiver.PacketReceiver, transactionReceiver packet_receiver.PacketReceiver, joinedTransactions *strings.Builder) {
	userMap := createUserMap(userReceiver)
	storeMap := createStoreMap(storeReceiver)

	transactions := transactionReceiver.GetPayload()
	lines := strings.Split(transactions, "\n")
	lines = lines[:len(lines)-1]
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 2 {
			panic("No hay 3 columnas como se esperaba")
		}
		// transactionID, storeID, userID
		// TODO: EN ESTA PORONGA EL USER ID ES UN FLOAT
		storeID, userID := cols[0], cols[1]
		storeName := storeMap[storeID]
		userBirthday, exits := userMap[userID]
		if !exits {
			userBirthday = "2002-12-08"
		}

		// Necesito algo del estilo: storeName, birthday
		fmt.Fprintf(joinedTransactions, "%s,%s\n", storeName, userBirthday)
	}
}
