package middleware

import (
	"fmt"
	"testing"
)

func TestWorkingQueue1To1(t *testing.T) {
	options := OptionsDefault()
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
