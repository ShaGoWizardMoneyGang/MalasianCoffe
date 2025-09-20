package main

import (
	"fmt"

	"system/middleware"
)

func main() {
	fmt.Println("hello world")

	options := middleware.ChannelOptionsDefault()
	// queue, _ := middleware.CreateQueue("go", options)
	fmt.Printf("{%+v}", options)
}
