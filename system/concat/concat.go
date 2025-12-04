package main

import (
	concat "malasian_coffe/system/concat/src"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

// Argumentos que recibe:
// 1. Address de rabbit
// 2. Nombre de la funcion que tiene que ejecutar
func main() {
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        sig := <-c
        println("Received signal:", sig.String())

        // Dump all goroutine stacks to stderr
        pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)

        os.Exit(1)
    }()



	rabbit_addr := os.Args[1]
	print("Rabbit address: ", rabbit_addr, "\n")


	routingKey_s := os.Args[2]

	concater    := concat.Concat{}
	concater.Build(rabbit_addr, routingKey_s)
	concater.Process()
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
