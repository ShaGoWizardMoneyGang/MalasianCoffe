package middleware

import (
	"fmt"
	"testing"
)

func TestWorkingQueue1To1(t *testing.T) {
	options := ChannelOptionsDefault()
	queue, err := CreateQueue("TestWorkingQueue1To1", options)
	if err != nil {
		panic("Creation of the queue not sucessful.")

	}

	// We create a consumer on a different go routine
	finish := make(chan bool)
	started := make(chan bool)
	go func() {
		msgs, _ := queue.StartConsuming()

		hasStarted := false
		for message := range *(*(msgs)) {
			// Starts reading messages
			if hasStarted == false {
				hasStarted = <-started
			}

			text := string(message.Body)
			fmt.Printf("Received package %s\n", text)

			if text != "TestMessage" {
				panic("The sent message does not match with the received message.")
			}

		}

		finish <- true
	}()

	info := []byte("TestMessage")
	new_err := queue.Send(info)

	// Already packet sent to the receiver
	started <- true

	if new_err != 0 {
		panic("Error: failed to send a message to the queue")
	}

	// We close the queue in order for the go routine to end
	queue.Close()

	// We wait for the go routine to finish
	<-finish
}

func TestDirectExchange1To1(t *testing.T) {
	opts := OptionsDefault()
	opts.routeKeys = []string{"info", "log", "labubu"}

	exchange, err := CreateExchange("TestDirectExchange1To1", opts)
	if err != nil {
		panic("Creation of the exchange not sucessful.")
	}

	finish := make(chan bool)
	started := make(chan bool)

	expected := len(opts.routeKeys)

	go func() {
		fmt.Println("[Test] Goroutine iniciada")

		msgs, _ := exchange.StartConsuming()

		hasStarted := false
		received := 0

		for m := range *(*(msgs)) {
			if !hasStarted {
				hasStarted = <-started
			}

			text := string(m.Body)
			fmt.Printf("Received package %s\n", text)

			if text != "TestMessage" {
				panic("The sent message does not match with the received message.")
			}

			received = received + 1
			if received == expected {
				_ = exchange.StopConsuming()
				break
			}
		}

		finish <- true
	}()

	payload := []byte("TestMessage")

	sendErr := exchange.Send(payload)

	if sendErr != 0 {
		panic("Error: failed to send a message to the exchange")
	}

	started <- true

	<-finish

	exchange.Close()
}

func TestWorkingQueue1ToN(t *testing.T) {
	queue, err := CreateQueue("TestWorkingQueue1ToN", ChannelOptionsDefault())
	if err != nil {
		panic("Creation of the queue not sucessful.")
	}

	N := 3  // Consumidores
	M := 18 // Mensajes

	// Consumidor listo
	ready := make(chan struct{}, N)
	// Sincronizacion para consumir
	start := make(chan struct{})
	// Aviso de consumicion
	consumer := make(chan string, M)

	// Go routine para cada consumidor
	for i := range N {
		id := fmt.Sprintf("consumidor%d", i+1)
		go func(id string) {
			msgs, _ := queue.StartConsuming()
			// Aviso de consumidor listo
			ready <- struct{}{}
			// Sincronizacion de arranque
			<-start
			// Se consumen todos los mensajes y se envía cada consumición
			for range *(*(msgs)) {
				consumer <- id
			}
		}(id)
	}

	// Se espera a que todos estén listos para arrancar
	for range N {
		<-ready
	}
	// Cuando están todos listos, arrancan en <-start
	close(start)

	// Se publican los M mensajes
	for range M {
		payload := []byte("TestMessage")
		sendErr := queue.Send(payload)
		if sendErr != 0 {
			panic("Error: failed to send a message to the queue")
		}

	}

	// Guardo quién va recibiendo mensajes
	distribution := map[string]int{}
	for range M {
		distribution[<-consumer]++
	}

	_ = queue.Close()

	// Verifico que hayan llegado entre todos los consumers exactamente M mensajes
	total := 0
	for _, c := range distribution {
		total += c
	}
	if total != M {
		panic("No llegaron los M mensajes enviados")
	}
	// Verifico que haya llegado al menos 1 mensaje a cada consumidor
	for _, c := range distribution {
		if c == 0 {
			panic("Hay un consumer que NO recibio nada")
		}
	}
	fmt.Printf("Disstribucion de los mensajes = %+v", distribution)
}

func TestDirectExchange1ToN(t *testing.T) {
	opts := OptionsDefault()
	opts.routeKeys = []string{"info"} // uso 1 sola para que no sea todo doble
	ex, err := CreateExchange("TestDirectExchange1ToN", opts)
	if err != nil {
		panic("Creation of the exchange not sucessful.")
	}

	N := 3  // Consumidores
	M := 18 // Mensajes

	// Consumidor listo
	ready := make(chan struct{}, N)
	// Sincronizacion para consumir
	start := make(chan struct{})
	// Aviso de consumicion
	consumer := make(chan string, M)

	// Go routine para cada consumidor
	for i := range N {
		id := fmt.Sprintf("consumidor%d", i+1)
		go func(id string) {
			msgs, _ := ex.StartConsuming()
			// Aviso de consumidor listo
			ready <- struct{}{}
			// Sincronizacion de arranque
			<-start
			// Se consumen todos los mensajes y se envía cada consumición
			for range *(*(msgs)) {
				consumer <- id
			}
		}(id)
	}

	// Se espera a que todos estén listos para arrancar
	for range N {
		<-ready
	}
	// Cuando están todos listos, arrancan en <-start
	close(start)

	// Se publican los M mensajes
	for range M {
		payload := []byte("TestMessage")
		sendErr := ex.Send(payload)
		if sendErr != 0 {
			panic("Error: failed to send a message to the exchange")
		}

	}

	// Guardo quién va recibiendo mensajes
	distribution := map[string]int{}
	for range M {
		distribution[<-consumer]++
	}

	_ = ex.Close()

	// Verifico que hayan llegado entre todos los consumers exactamente M mensajes
	total := 0
	for _, c := range distribution {
		total += c
	}
	if total != M {
		panic("No llegaron los M mensajes enviados")
	}
	// Verifico que haya llegado al menos 1 mensaje a cada consumidor
	for _, c := range distribution {
		if c == 0 {
			panic("Hay un consumer que NO recibio nada")
		}
	}
	fmt.Printf("Disstribucion de los mensajes = %+v", distribution)
}
