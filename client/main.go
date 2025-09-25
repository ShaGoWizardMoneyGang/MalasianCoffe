package main

import (
	"errors"
	"fmt"
	// "io"
	"os"

	"bufio"

	"malasian_coffe/packet"
	// "malasian_coffe/protocol"
)

const (
	// Max batch size is 8 KILO bytes aka 8 thousand bytes
	MAX_BATCH_SIZE int  = 8000
)

func createPackagesFrom(dir string, dirID uint, session_ID uint32, listen_addr string) {
	packetBuilder := packet.NewPacketBuilder(dirID, session_ID, listen_addr)
	payloadBuffer := make([]byte, MAX_BATCH_SIZE)

	entries, err := os.ReadDir(dir)

	if err != nil {
		fmt.Printf("Failed to read directory: {}\n", err)
		panic("")
	}

	for _, file := range entries {
		if file.IsDir() {
			fmt.Printf("WARNING: Found subdirectory {} in directory {}", file, dir)
			continue
		}
		csv_file, err := os.Open(dir + "/" + file.Name())
		if err != nil {
			return packet.Packet{}, errors.New(fmt.Sprintf("Couldn't open csv file in dir {}, because of {}", dir, err))
		}
		csv_reader := bufio.NewScanner(csv_file);
		{
			// Skip first line which holds column names
			csv_reader.Scan()
		}

		for csv_reader.Scan() {
			register := csv_reader.Text()
			register_b := []byte(register)
			if len(payloadBuffer) + len(register_b) > MAX_BATCH_SIZE {
				// Batch full, send it and clear the buffer
				packetBuilder.CreatePacket(payloadBuffer, false)
				payloadBuffer = []byte{0}
			}
			payloadBuffer = append(payloadBuffer, register_b...)
		}
	}
     packetBuilder.CreatePacket(payloadBuffer, true)
}

func sendPacket(packet packet.Packet, addr string) {
	
}

func main() {
	// Directory with all dataset subdirectories
	dataset_directory := os.Args[1]
	addr := os.Args[2]
	listen_addr := os.Args[3]

	// TODO: Obtener del gateway
	session_id := uint32(0)


	entries, err := os.ReadDir(dataset_directory)
	if err != nil {
		fmt.Printf("Failed to read directory: {}\n", err)
		panic("")
	}

	dirID := uint(0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subDirPath := dataset_directory + entry.Name()
		packet, err := createPackagesFrom(subDirPath, dirID, session_id, listen_addr)
		if err != nil {
			 panic(err)
		}

		sendPacket(packet, addr)
		dirID += 1
	}
	fmt.Println("hello world")
}

