package joiner

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/packets/multiple_packet_receiver"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	watchdog "malasian_coffe/system/watchdog/src"
	"malasian_coffe/utils/colas"
)

type joinerQuery3 struct {
	inputChannel chan colas.PacketMessage

	outputChannel chan packet.Packet

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery3 *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}

func joinQuery3(sessionID string, inputChannel <-chan colas.PacketMessage, outputChannel chan<- packet.Packet) {
	expected_datasets := []multiple_packet_receiver.NombreDataset {
		multiple_packet_receiver.NombreDataset("stores"),
		multiple_packet_receiver.NombreDataset("transactions"),
	};

	packet_receiver := multiple_packet_receiver.NewMultiplePacketReceiver(sessionID, expected_datasets, joinerFunctionQuery3)

	var joinedTransactions string

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

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"stores", "transactions"}, []string{joinedTransactions})

	for _, pkt := range pkt_joineado {
		bitacora.Info(fmt.Sprintf("Envio pkt joineado al sender, session: %s", pkt.GetSessionID()))
		outputChannel <- pkt
	}

	packet_receiver.Clean()
}

func (jq3 *joinerQuery3) Build(rabbitAddr string, routingKey string) {
	jq3.inputChannel = make(chan colas.PacketMessage)
	jq3.outputChannel = make(chan packet.Packet)

	print("[joiner] buildeo con routing key: ", routingKey)
	jq3.colaStoresInput = colas.InstanceQueueRouted("FilteredStores3", rabbitAddr, routingKey)
	jq3.colaAggTransInput = colas.InstanceQueueRouted("GlobalAggregation3", rabbitAddr, routingKey)

	jq3.colaSalidaQuery3 = colas.InstanceQueue("SalidaQuery3", rabbitAddr)

	jq3.sessionHandler = sessionhandler.NewSessionHandler(joinQuery3, jq3.outputChannel)
}

func (jq3 *joinerQuery3) Process() {
	slog.Info("Arranca procesamiento del joiner 3")
	// Stores
	go colas.InputQueue(jq3.colaStoresInput, jq3.inputChannel)

	go colas.InputQueue(jq3.colaAggTransInput, jq3.inputChannel)

	watchdog := watchdog.CreateWatchdogListener()
	healthcheckChannel := make(chan string)
	go watchdog.Listen(healthcheckChannel)

	for {
		select {
		case inputPacket := <-jq3.inputChannel:
			jq3.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq3.outputChannel:
			jq3.colaSalidaQuery3.Send(packetJoineado)
		case responseAddress := <-healthcheckChannel:
			IP := strings.Split(responseAddress, ":")[0]
			fmt.Println("Joiner Query3 received healthcheck ping from", IP)
			//PARA SIMULAR QUE NO MANDA PONG DESPUES DE REINTENTAR
			time.Sleep(5 * time.Second)
			watchdog.Pong(IP)
		}
	}
}

func joinerFunctionQuery3(inputs map[multiple_packet_receiver.NombreDataset]multiple_packet_receiver.ContenidoCompleto) string {
	storeMap := createStoreMap(string(inputs["stores"]))
	transactions := string(inputs["transactions"])

	var joinedTransactions strings.Builder

	lines := strings.Split(transactions, "\n")
	lines = lines[:len(lines)-1]

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		semester, storeID, tpv := cols[0], cols[1], cols[2]
		storeName := storeMap[storeID]

		fmt.Fprintf(&joinedTransactions, "%s,%s,%s\n", semester, storeName, tpv)
	}

	final_result := joinedTransactions.String()

	return final_result
}
