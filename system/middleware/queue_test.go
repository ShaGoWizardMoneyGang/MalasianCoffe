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
		t.Fatalf("Creation of the exchange not successful: %v", err)
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
		t.Fatalf("Error: failed to send a message to the exchange (err=%v)", sendErr)
	}

	started <- true

	<-finish

	exchange.Close()
}

// func TestWorkingQueue1ToN(t *testing.T) {
// 	options := OptionsDefault()
// 	queue, err := CreateQueue("TestWorkingQueue1ToN", options)
// 	if err != nil {
// 		panic("Creation of the queue not sucessful.")

// 	}

// 	// We create a consumer on a different go routine
// 	finish := make(chan bool)
// 	started := make(chan bool)
// 	go func() {
// 		msgs, _ := queue.StartConsuming()

// 		hasStarted := false
// 		for message := range *(*(msgs)) {
// 			// Starts reading messages
// 			if hasStarted == false {
// 				hasStarted = <-started
// 			}

// 			text := string(message.Body)
// 			fmt.Printf("Received package %s\n", text)

// 			if text != "TestMessage" {
// 				panic("The sent message does not match with the received message.")
// 			}

// 		}

// 		finish <- true
// 	}()

// 	info := []byte("TestMessage")
// 	new_err := queue.Send(info)

// 	// Already packet sent to the receiver
// 	started <- true

// 	if new_err != 0 {
// 		panic("Error: failed to send a message to the queue")
// 	}

// 	// We close the queue in order for the go routine to end
// 	queue.Close()

// 	// We wait for the go routine to finish
// 	<-finish
// }
