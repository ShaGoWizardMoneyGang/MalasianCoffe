package main

import (
	"bytes"
	"os"

	"malasian_coffe/packets/packet"
	"malasian_coffe/utils/disk"
)

// Tool usada para des-serializar un paquete y mostrarlo por la CLI.
func main() {
	path := os.Args[1]

	serialized_packet, err := disk.ReadBytes(path)
	if err != nil {
		panic(err)
	}

	reader := bytes.NewReader(serialized_packet)

	deserialized_packet, err := packet.DeserializePackage(reader)
	if err != nil {
		panic(err)
	}

	repr := deserialized_packet.ToString()
	println(repr)
}
