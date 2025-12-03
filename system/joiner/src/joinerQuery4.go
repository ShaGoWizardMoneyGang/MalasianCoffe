package joiner

import (
	"fmt"
	"strings"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/multiple_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
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

func createUserMap(userPayload string) map[string]string {
	lines := strings.Split(userPayload, "\n")
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


func joinQuery4(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	expected_datasets := []multiple_packet_receiver.NombreDataset {
		multiple_packet_receiver.NombreDataset("stores"),
		multiple_packet_receiver.NombreDataset("users"),
		multiple_packet_receiver.NombreDataset("transactions"),
	};

	packet_receiver := multiple_packet_receiver.NewMultiplePacketReceiver(sessionID, expected_datasets, joinerFunctionQuery4)

	// Resultado final
	var joinedTransactions string

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pktMsg := <-inputChannel
		pkt := pktMsg.Packet

		received_all := packet_receiver.ReceivePacket(pktMsg)

		if !received_all {
			continue
		}

		joinedTransactions = packet_receiver.GetPayload()
		last_packet = pkt
		break
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"users", "stores", "transactions"}, []string{joinedTransactions})

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}

	colas.WaitForAnswer(inputChannel)
	packet_receiver.Clean()
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
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-jq4.inputChannel:
			jq4.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq4.outputChannel:
			jq4.colaSalidaQuery4.Send(packetJoineado)
			ackPkt := colas.NewAnswerPacket(packetJoineado)
			jq4.sessionHandler.PassPacketToSession(ackPkt)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Joiner Query4 received healthcheck ping from", IP)
			//PARA SIMULAR QUE NO MANDA PONG DESPUES DE REINTENTAR
			//time.Sleep(15 * time.Second)
			watchdog.Pong(IP)
		}
	}
}

func joinerFunctionQuery4(inputs map[multiple_packet_receiver.NombreDataset]multiple_packet_receiver.ContenidoCompleto) string {
	userMap := createUserMap(string(inputs["user"]))
	storeMap := createStoreMap(string(inputs["store"]))
	transactions := string(inputs["transaction"])

	var joinedTransactions strings.Builder

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
		fmt.Fprintf(&joinedTransactions, "%s,%s\n", storeName, userBirthday)
	}

	final_result := joinedTransactions.String()

	return final_result
}
