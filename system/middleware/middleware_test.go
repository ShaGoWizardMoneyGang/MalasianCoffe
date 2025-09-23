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

			received = received + 1   // Una vez que recibo todo lo que espero corto
			if received == expected { // PORQUE NO HAY WHILE EN GO ME HACE MAL
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
	q, err := CreateQueue("TestWorkingQueue1ToN", ChannelOptionsDefault())
	if err != nil {
		panic("Creation of the queue not sucessful.")
	}

	N := 3  //consumidores
	M := 18 //mensajes

	// LOCURA: el tipo de canal struct sirve cuando no me interesa mandar datos, sino un aviso de che loco aca paso algo

	//canal para avisar que un consumidor esta listo
	// buffer de tamaño N, hago hasta N escrituras
	ready := make(chan struct{}, N)
	//canal para saber si se arranco
	start := make(chan struct{})
	// aviso por cada mensaje recibido
	// este es de tipo string porque voy a mandar el id del consumer
	// buffer de tamaño M, hago hasta M escrituras
	lecturaDeMensaje := make(chan string, M)

	// para cada consumidor se lanza una go routine
	for i := range N {
		// para mas facil lectura
		id := fmt.Sprintf("consumidor%d", i+1)
		//
		go func(id string) {
			msgs, _ := q.StartConsuming()
			//aviso que ya arranque a consumir
			ready <- struct{}{}
			// espero a que todos arranquen
			<-start
			// consumo todo y por cada mensaje mando mi id al canal
			for range *(*(msgs)) {
				lecturaDeMensaje <- id
			}
		}(id)
	}

	// esperar a que estén listos
	for range N {
		<-ready
	}
	//cuando tengo a todos ready, todas las go routines se desbloquean en el <-start
	close(start)

	// publico los M mensajes
	for range M {
		payload := []byte("TestMessage")
		sendErr := q.Send(payload)
		if sendErr != 0 {
			panic("Error: failed to send a message to the queue")
		}

	}

	// me guardo quien va recibiendo mensajes
	distribucion := map[string]int{}
	for range M {
		//en este map voy sumando cuando got tiene un id
		distribucion[<-lecturaDeMensaje]++
	}

	_ = q.Close()

	// verifico que hayan llegado entre todos los consumers exactamente M mensajes
	total := 0
	for _, cantidad := range distribucion {
		total += cantidad
	}
	if total != M {
		panic("No llegaron los M mensajes enviados")
	}
	// verifico que haya llegado al menos 1 mensaje a cada consumidor
	for _, cantidadMensajes := range distribucion {
		if cantidadMensajes == 0 {
			panic("Hay un consumer que NO recibio nada")
		}
	}
	fmt.Printf("Disstribucion de los mensajes = %+v", distribucion)
}

func TestDirectExchange1ToN(t *testing.T) {
	opts := OptionsDefault()
	opts.routeKeys = []string{"info"} // uso 1 sola para que no sea todo doble
	ex, err := CreateExchange("TestDirectExchange1ToN", opts)
	if err != nil {
		panic("Creation of the exchange not sucessful.")
	}

	N := 3  //consumidores
	M := 18 //mensajes

	// LOCURA: el tipo de canal struct sirve cuando no me interesa mandar datos, sino un aviso de che loco aca paso algo
	//canal para avisar que un consumidor esta listo
	// buffer de tamaño N, hago hasta N escrituras
	ready := make(chan struct{}, N)
	//canal para saber si se arranco
	start := make(chan struct{})
	// aviso por cada mensaje recibido
	// este es de tipo string porque voy a mandar el id del consumer
	// buffer de tamaño M, hago hasta M escrituras
	lecturaDeMensaje := make(chan string, M)

	// para cada consumidor go routine
	for i := range N {
		id := fmt.Sprintf("consumidor%d", i+1)
		go func(id string) {
			msgs, _ := ex.StartConsuming()
			ready <- struct{}{}
			<-start
			for range *(*(msgs)) {
				lecturaDeMensaje <- id
			}
		}(id)
	}

	for range N {
		<-ready
	}
	close(start)

	// mando M mensajes
	for range M {
		payload := []byte("TestMessage")
		sendErr := ex.Send(payload)
		if sendErr != 0 {
			panic("Error: failed to send a message to the exchange")
		}

	}

	distribucion := map[string]int{}
	for range M {
		distribucion[<-lecturaDeMensaje]++
	}

	_ = ex.StopConsuming()
	_ = ex.Close()

	// verifico que hayan llegado entre todos los consumers exactamente M mensajes
	total := 0
	for _, cantidad := range distribucion {
		total += cantidad
	}
	if total != M {
		panic("No llegaron los M mensajes enviados")
	}
	// verifico que haya llegado al menos 1 mensaje a cada consumidor
	for _, cantidadMensajes := range distribucion {
		if cantidadMensajes == 0 {
			panic("Hay un consumer que NO recibio nada")
		}
	}
	fmt.Printf("Disstribucion de los mensajes = %+v", distribucion)
}
