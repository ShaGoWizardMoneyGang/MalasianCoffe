package joiner

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	sessionhandler "malasian_coffe/system/session_handler"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery4 struct {
	inputChannel   chan packet.Packet

	outputChannel   chan packet.Packet

	colaStoresInput   *middleware.MessageMiddlewareQueue
	colaUsersInput   *middleware.MessageMiddlewareQueue
	colaAggTransInput *middleware.MessageMiddlewareQueue

	colaSalidaQuery4  *middleware.MessageMiddlewareQueue

	// Tenemos una go routine por cada session
	sessionHandler sessionhandler.SessionHandler
}


func createUserMap(userReceiver packet.PacketReceiver) map[string]string {
	storePkt := userReceiver.GetPayload()
	lines    := strings.Split(storePkt, "\n")
	lines     = lines[:len(lines)-1]

	// Le damos un tamano inicial de lines porque deberia tener un tamano igual
	// al de la cantidad de lineas. Ademas, ya pre-alocamos la memoria.
	storeID2Name        := make(map[string]string, len(lines))

	for _, line := range lines {
		// store_id , store_name
		cols := strings.Split(line, ",")
		store_id, store_name := cols[0], cols[1]
		storeID2Name[store_id] = store_name
	}

	return storeID2Name
}

func joinQuery4(inputChannel <-chan packet.Packet, outputChannel chan<- packet.Packet) {
	storeReceiver       := packet.NewPacketReceiver()

	userReceiver        := packet.NewPacketReceiver()

	// Aca me guardo todos los packets de transactions que llegaron antes de los
	// stores. Deberian ser pocos (si es que existen)
	transactionReceiver := packet.NewPacketReceiver()

	// Resultado final
	var joinedTransactions strings.Builder

	// Nos guardamos el ultimo paquete para extraer la metadata, la dulce y
	// jugosa metadata
	var last_packet packet.Packet

	for {
		pkt :=  <- inputChannel

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "stores" {
			storeReceiver.ReceivePacket(pkt)
		} else if dataset_name == "users" {
			userReceiver.ReceivePacket(pkt)
		} else if dataset_name == "transactions" {
			transactionReceiver.ReceivePacket(pkt)

		} else {
			panic(fmt.Errorf("JoinerQuery4 received packet from dataset that was not expecting: %s", dataset_name))
		}

		// No joineamos hasta tenerlo todo.
		if storeReceiver.ReceivedAll() && userReceiver.ReceivedAll() && transactionReceiver.ReceivedAll() {
			slog.Info("Comienza proceso de join")
			joinerFunctionQuery4New(storeReceiver, userReceiver, transactionReceiver, &joinedTransactions)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"users", "stores", "transactions"}, []string{joinedTransactions.String()})

	// Liberamos
	joinedTransactions.Reset()

	slog.Info("Envio pkt joineado al sender")
	// Deberia ser 1 solo
	for _, pkt := range pkt_joineado {
		fmt.Printf("%+v\n", pkt)
		outputChannel <- pkt
	}
}

func (jq4 *joinerQuery4) Build(rabbitAddr string) {
	jq4.inputChannel          = make(chan packet.Packet)

	jq4.outputChannel         = make(chan packet.Packet)

	jq4.colaStoresInput       = colas.InstanceQueue("FilteredStores4", rabbitAddr)
	jq4.colaUsersInput        = colas.InstanceQueue("FilteredUsers4", rabbitAddr)
	jq4.colaAggTransInput     = colas.InstanceQueue("GlobalAggregation4", rabbitAddr)

	jq4.colaSalidaQuery4      = colas.InstanceQueue("SalidaQuery4", rabbitAddr)

	jq4.sessionHandler        = sessionhandler.NewSessionHandler(joinQuery4, jq4.outputChannel)

}

func (jq4 *joinerQuery4) Process() {
	go inputQueue(jq4.colaStoresInput, jq4.inputChannel)

	go inputQueue(jq4.colaUsersInput, jq4.inputChannel)

	go inputQueue(jq4.colaAggTransInput, jq4.inputChannel)

	for {
		select {
		case inputPacket := <-jq4.inputChannel:
			jq4.sessionHandler.PassPacketToSession(inputPacket)
		case packetJoineado := <-jq4.outputChannel:
			jq4.colaSalidaQuery4.Send(packetJoineado.Serialize())
		}
	}
}

func joinerFunctionQuery4New(storeReceiver packet.PacketReceiver,
	userReceiver packet.PacketReceiver,
	transactionReceiver packet.PacketReceiver,
	joinedTransactions *strings.Builder) {

	userMap     := createUserMap(userReceiver)
	storeMap    := createStoreMap(storeReceiver)

	transactions := transactionReceiver.GetPayload()
	lines := strings.Split(transactions, "\n")
	lines = lines[:len(lines)-1]
	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 2 {
			panic("No hay 3 columnas como se esperaba")
		}

		storeID, userID := cols[0], cols[1]
		storeName    := storeMap[storeID]
		userBirthday, exits := userMap[userID]
		if !exits {
			userBirthday = "2002-12-08"
		}

		// Necesito algo del estilo: storeName, birthday
		fmt.Fprintf(joinedTransactions, "%s,%s\n", storeName, userBirthday)
	}
}
