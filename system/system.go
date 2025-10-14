package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"malasian_coffe/bitacora"
	"malasian_coffe/packets/packet"
	"malasian_coffe/system/middleware"
	"malasian_coffe/utils/dataset"
	"malasian_coffe/utils/network"
)

func main() {
	rabbit_addr := os.Args[2]
	//QUERY 2 USA TRANSACTIONS Y STORES
	colaTransactions, err := middleware.CreateQueue("DataTransactions", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaUsers, err := middleware.CreateQueue("DataUsers", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaStores, err := middleware.CreateQueue("DataStores", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaTransactionItems, err := middleware.CreateQueue("DataTransactionItems", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	colaMenuItems, err := middleware.CreateQueue("DataMenuItems", middleware.ChannelOptions{DaemonAddress: network.AddrToRabbitURI(rabbit_addr)})
	if err != nil {
		panic(fmt.Errorf(`failed to rconnect to RabbitMQ: %s. Is the daemon active?
		Try running:

		sudo systemctl start rabbitmq
		or
		sudo rc-service rabbitmq start`, rabbit_addr))
	}

	//listen_addr
	listen_addr := os.Args[1]

	list, err := net.Listen("tcp", listen_addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener. Error: %s", err))
	}
	conn, _ := list.Accept()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		for {
			packet_b, err := network.ReceiveFromNetwork(conn)
			packet_reader := bytes.NewReader(packet_b)
			if err != nil {
				panic(err)
			}
			packet, err := packet.DeserializePackage(packet_reader)
			if err != nil {
				fmt.Errorf("Error deserializing package: %w", err)
			}

			// TODO: esto esta hardcodeado asi porque es para la query 1.
			// Aca deberia haber un switch que lo envie a la queue correspondiente
			packet_id, err := strconv.ParseUint(packet.GetDirID(), 10, 64)
			dataset_name, err := dataset.IDtoDataset(packet_id)
			if err != nil {
				panic(err)
			}
			switch dataset_name {
			case "menu_items":
				slog.Debug("Envio a cola de menu items")
				colaMenuItems.Send(packet)
			case "stores":
				slog.Debug("Envio a cola de stores")
				colaStores.Send(packet)
			case "transaction_items":
				slog.Debug("Envio a cola de transaccions items")
				colaTransactionItems.Send(packet)
			case "transactions":
				slog.Debug("Envio a cola de transactions")
				colaTransactions.Send(packet)
			case "users":
				slog.Debug("Envio a cola de users")
				colaUsers.Send(packet)
			}
		}
	}()

	select {
	case <-ctx.Done():
		bitacora.Info("Graceful shutdown solicitado (SIGTERM/SIGINT)")
	}

}
