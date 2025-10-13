package main

import (
	"context"
	"malasian_coffe/bitacora"
	concat "malasian_coffe/system/concat/src"
	"os"
	"os/signal"
	"syscall"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {
	rabbit_addr := os.Args[1]
	print("Rabbit address: ", rabbit_addr, "\n")

	routingKey_s := os.Args[2]

	concater := concat.Concat{}
	concater.Build(rabbit_addr, routingKey_s)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	done := make(chan struct{})
	go func() {
		concater.Process()
		close(done)
	}()

	select {
	case <-ctx.Done():
		bitacora.Info("Graceful shutdown solicitado (SIGTERM/SIGINT)")
	case <-done:
		bitacora.Info("Procesamiento finalizado normalmente")
	}

}

// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣤⣤⡄
// ⠀⣴⣶⣶⣶⡶⠶⠶⡶⣶⣦⣄⠀⠀⠀⢀⣴⣶⠶⢶⣶⠀⢠⡴⠶⠶⢶⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⡾⠶⠶⢶⣶⡶⠶⠶⠶⠶⢶⣿⣿⡋⠉⠉⣹⡇
// ⠈⣿⣿⠀⣄⢠⣤⣤⣄⣈⠙⣿⣷⠀⠀⣼⡿⠁⠀⠀⢻⣆⢸⡇⠀⠀⢸⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⠃⠀⠀⢸⣿⠁⠀⠀⠀⠀⠀⠘⢿⣿⠀⠀⣿⠇
// ⠀⣿⣿⠀⡏⠘⢋⡟⠋⠉⠀⣨⣿⡆⣼⣿⠁⠀⠀⠀⠈⣿⡄⣷⠀⠀⠈⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡜⠀⠀⠀⣾⠃⠀⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⣿⠀
// ⠀⣿⣿⠀⣇⣼⣿⠟⠀⢀⣰⣿⠟⡰⢿⠃⠀⠀⠀⠀⠀⢹⣷⣻⠀⠀⠀⢻⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⡇⠀⠀⢠⡿⠀⠀⠀⠀⠀⠀⠀⠀⣸⡟⠀⢰⣿⠀
// ⠀⣿⡿⠀⡀⣀⣀⣤⣶⣾⣿⢃⢰⣾⠏⠀⠀⠀⠀⠀⠀⠀⢻⣿⡄⠀⠀⢸⣇⠀⢠⣿⠟⠋⣷⣆⠀⢠⡇⠀⠀⣼⡇⠀⠀⠀⠀⣀⣤⣤⣴⣿⠃⠀⢸⡇⠀
// ⠀⣿⡇⠀⠈⠉⠻⣿⣷⡀⠀⠀⣼⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣇⠀⠀⠘⣿⣠⡿⠁⣀⣤⠸⢿⡄⢸⠀⠀⠀⣿⠀⠀⣿⡀⠀⠹⣧⠀⠈⣿⠀⠀⣾⡇⠀
// ⠀⣿⡇⠀⢰⣆⠀⠀⢻⣷⡀⣰⠏⠀⠀⠀⣀⣀⣀⣀⣀⡀⠀⠸⣿⠀⠀⠀⣿⡟⠁⠀⣾⣿⡀⠘⣷⣼⠀⠀⢰⣿⠀⠀⣿⣧⠀⠀⢻⣧⠀⣿⠀⠀⣿⠃⠀
// ⠀⣿⡇⠀⢸⡏⣶⣥⠈⢿⣿⡿⠀⠀⢰⡿⠛⠛⠋⠉⢻⣇⠀⠀⢹⡄⠀⠀⠉⠁⠀⣼⠏⠹⣇⠀⠹⣿⠀⠀⢸⡟⠀⠀⣿⠻⣧⠀⠀⢻⣧⣸⣧⣠⣿⠀⠀
// ⠀⣿⣇⠀⢸⡇⠹⣿⣶⠈⠻⣧⠀⢀⣿⠇⠀⠀⠀⠀⠀⢿⣇⠀⠈⣷⡀⠀⠀⠀⣼⡏⠀⠀⢿⡆⠀⠀⠀⠀⣼⣷⠀⠀⢸⠀⢻⣧⠀⠀⢻⣏⢉⢉⣿⡄⠀
// ⠀⣿⣿⣦⣿⡇⠀⠹⣿⣧⣠⣾⡇⣻⡟⠀⠀⠀⠀⠀⠀⠘⣿⣆⠀⠸⠇⠀⠀⣼⡟⠀⠀⠀⠘⣿⡀⠀⠀⢀⡿⣿⠀⠀⢸⠀⠀⢻⣧⡀⠀⣻⣶⣤⣿⡇⠀
// ⠀⠻⠿⠛⠛⠃⠀⠀⠻⠿⠿⠿⠿⠛⠁⠀⠀⠀⠀⠀⠀⠀⠘⠛⠛⠻⠿⠷⠾⠟⠀⠀⠀⠀⠀⠙⠿⠿⠷⠾⠟⠿⠿⠿⠟⠃⠀⠀⠛⠿⠿⠿⠿⠛⠛⠁⠀
