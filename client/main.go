package main

import (
	"errors"
	"fmt"
	"net"

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

func sendToSocket(conn *net.Conn, data []byte) error {
	length := len(data)

	var sent = 0
	var err error
	for offset := 0 ; offset < length ; offset += sent {
		sent, err = (*conn).Write(data[offset:])
		if err != nil {
			return err
		}
	}

	return nil
}

func createPackagesFrom(dir string, dirID uint, session_ID uint64, listen_addr string, send_addr *net.Conn) (error) {
	packetBuilder := packet.NewPacketBuilder(dirID, session_ID, listen_addr)
	payloadBuffer := make([]byte, MAX_BATCH_SIZE)
	used_size := 0

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
			return errors.New(fmt.Sprintf("Couldn't open csv file in dir {}, because of {}", dir, err))
		}
		csv_reader := bufio.NewScanner(csv_file);
		{
			// Skip first line which holds column names
			csv_reader.Scan()
		}

		for csv_reader.Scan() {
			register := csv_reader.Text()
			register_b := []byte(register)
			if used_size + len(register_b) > MAX_BATCH_SIZE {
				// Batch full, send it and clear the buffer
				packet, err := packetBuilder.CreatePacket(payloadBuffer, false)
				if err != nil {
					return err
				}
				sendToSocket(send_addr, packet.Serialize())
				payloadBuffer = []byte{0}
				used_size = 0
			}
			payloadBuffer = append(payloadBuffer, register_b...)
			used_size += len(register_b)
		}
	}
     packet, err := packetBuilder.CreatePacket(payloadBuffer, true)
	if err != nil {
		return err
	}
	sendToSocket(send_addr, packet.Serialize())
	return nil
}

func main() {
	// Directory with all dataset subdirectories
	dataset_directory := os.Args[1]

	addr := os.Args[2]
	conn, err := net.Dial("tcp", addr)

	listen_addr := os.Args[3]

	// TODO: Obtener del gateway
	session_id := uint64(0)


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
		err := createPackagesFrom(subDirPath, dirID, session_id, listen_addr, &conn)
		if err != nil {
			 panic(err)
		}

		dirID += 1
	}
	fmt.Println("hello world")
}

