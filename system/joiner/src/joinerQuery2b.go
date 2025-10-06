package joiner

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/colas"
	"malasian_coffe/utils/dataset"
)

type joinerQuery2b struct {
	// Tenemos una go routine por cada session
	sessions map[string](chan packet.Packet)

	colaMenuItemsInput *middleware.MessageMiddlewareQueue
	colaAggItemsInput  *middleware.MessageMiddlewareQueue

	colaSalidaQuery2b *middleware.MessageMiddlewareQueue
}

func joinQuery2b(inputChannel chan packet.Packet, outputQueue *middleware.MessageMiddlewareQueue) {
	menuItemReceiver := packet.NewPacketReceiver()

	transactionItemReceiver := packet.NewPacketReceiver()

	var joinedTransactionItems strings.Builder

	var last_packet packet.Packet

	for {
		pkt := <-inputChannel
		fmt.Printf("Recibi %v\n", pkt)

		packet_id, err := strconv.ParseUint(pkt.GetDirID(), 10, 64)
		dataset_name, err := dataset.IDtoDataset(packet_id)
		if err != nil {
			panic(err)
		}

		if dataset_name == "menu_items" {
			menuItemReceiver.ReceivePacket(pkt)
		} else if dataset_name == "transaction_items" {
			transactionItemReceiver.ReceivePacket(pkt)
		} else {
			panic(fmt.Errorf("JoinerQuery2b received packet from dataset that was not expecting: %s", dataset_name))
		}

		if menuItemReceiver.ReceivedAll() && transactionItemReceiver.ReceivedAll() {
			slog.Info("Comienza proceso de join")
			joinerFunctionQuery2bNew(menuItemReceiver, transactionItemReceiver, &joinedTransactionItems)

			last_packet = pkt
			break
		}
	}

	pkt_joineado := packet.ChangePayloadJoin(last_packet, []string{"menu_items", "transaction_items"}, []string{joinedTransactionItems.String()})

	joinedTransactionItems.Reset()

	slog.Info("Envio pkt joineado al sender")
	for _, pkt := range pkt_joineado {
		fmt.Printf("%+v\n", pkt)
		outputQueue.Send(pkt.Serialize())
	}
}

func (jq2b *joinerQuery2b) passPacketToJoiner(pkt packet.Packet) {
	slog.Info("Envio packete a la session")
	sessionID := pkt.GetSessionID()
	channel, exists := jq2b.sessions[sessionID]

	if !exists {
		// Joiner
		slog.Info("Creo un hilo joiner")
		assigned_channel := make(chan packet.Packet)
		go joinQuery2b(assigned_channel, jq2b.colaSalidaQuery2b)

		jq2b.sessions[sessionID] = assigned_channel

		channel = assigned_channel
	}

	channel <- pkt
}

func (jq2b *joinerQuery2b) Build(rabbitAddr string) {
	slog.Info("Inicializo el Joiner 2b")
	sessionHandler := make(map[string](chan packet.Packet))

	colaSalidaQuery2b := colas.InstanceQueue("SalidaQuery2b", rabbitAddr)

	colaMenuItemsInput := colas.InstanceQueue("FilteredMenuItems2b", rabbitAddr)
	colaAggTransItems := colas.InstanceQueue("GlobalAggregation2b", rabbitAddr)

	jq2b.sessions = sessionHandler

	jq2b.colaMenuItemsInput = colaMenuItemsInput
	jq2b.colaAggItemsInput = colaAggTransItems

	jq2b.colaSalidaQuery2b = colaSalidaQuery2b
}

func (jq2b *joinerQuery2b) Process() {
	slog.Info("Arranca procesamiento del joiner 2b")
	menuItemListener := make(chan packet.Packet)
	aggregatorGlobalListener := make(chan packet.Packet)

	go func() {
		colasEntrada := jq2b.colaMenuItemsInput

		messages := colas.ConsumeInput(colasEntrada)
		for message := range *messages {
			slog.Info("Recibi mensaje de cola de menu items")
			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			menuItemListener <- pkt
		}
	}()

	go func() {
		colasEntrada := jq2b.colaAggItemsInput

		messages := colas.ConsumeInput(colasEntrada)

		for message := range *messages {
			slog.Info("Recibi mensaje de cola de aggregated filtered transaction items")

			packetReader := bytes.NewReader(message.Body)
			pkt, _ := packet.DeserializePackage(packetReader)

			err := message.Ack(false)
			if err != nil {
				panic(fmt.Errorf("Could not ack, %w", err))
			}

			aggregatorGlobalListener <- pkt
		}
	}()

	for {
		select {
		case menuItemPacket := <-menuItemListener:
			jq2b.passPacketToJoiner(menuItemPacket)
		case aggregatedPacket := <-aggregatorGlobalListener:
			jq2b.passPacketToJoiner(aggregatedPacket)
		}
	}
}

func joinerFunctionQuery2bNew(menuItemReceiver packet.PacketReceiver, transactionItemReceiver packet.PacketReceiver, joinedTransactionItems *strings.Builder) {
	menuItemMap := createMenuItemMap(menuItemReceiver)

	transactionItems := transactionItemReceiver.GetPayload()
	lines := strings.Split(transactionItems, "\n")
	lines = lines[:len(lines)-1]

	for _, r := range lines {
		cols := strings.Split(r, ",")
		if len(cols) < 3 {
			panic("No hay 3 columnas como se esperaba")
		}
		yearMonth, itemID, sellings_qty := cols[0], cols[1], cols[2]
		itemName := menuItemMap[itemID]

		fmt.Fprintf(joinedTransactionItems, "%s,%s,%s\n", yearMonth, itemName, sellings_qty)
	}
}
