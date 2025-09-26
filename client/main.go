package main

import (
	"errors"
	"fmt"
	"net"
	"strings"

	// "io"
	"os"

	"bufio"

	"malasian_coffe/packet"
	// "malasian_coffe/protocol"
)

const (
	// Max batch size es 8192 simplemente porque es el valor default de BUFSIZ en glibc:
	// https://sourceware.org/git/?p=glibc.git;a=blob;f=libio/stdio.h;h=e0e70945fab175fafcb0c8bbae96ad7eebe3df5a;hb=HEAD#l100
	// Ademas, en el tp0 el maximo era 8000, el cual es parecido en tamano
	MAX_BATCH_SIZE int  = 8192
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
	// payloadBuffer := make([]byte, MAX_BATCH_SIZE)
	var payloadBuffer strings.Builder
	payloadBuffer.Grow(MAX_BATCH_SIZE)

	entries, err := os.ReadDir(dir)

	if err != nil {
		fmt.Printf("Failed to read directory: {%s}\n", err)
		panic("")
	}

	for _, file := range entries {
		if file.IsDir() {
			fmt.Printf("WARNING: Found subdirectory {%s} in directory {%s}", file, dir)
			continue
		}
		csv_file, err := os.Open(dir + "/" + file.Name())
		if err != nil {
			return errors.New(fmt.Sprintf("Couldn't open csv file in dir {%s}, because of {%s}", dir, err))
		}
		csv_reader := bufio.NewScanner(csv_file);
		{
			// Skip first line which holds column names
			csv_reader.Scan()
		}

		for csv_reader.Scan() {
			register := csv_reader.Text() + "\n"
			if payloadBuffer.Len() + len(register) > MAX_BATCH_SIZE {
				// Batch full, send it and clear the buffer
				packet, err := packetBuilder.CreatePacket(payloadBuffer.String(), false)
				if err != nil {
					return err
				}
				sendToSocket(send_addr, packet.Serialize())
				payloadBuffer.Reset()
			}
			payloadBuffer.WriteString(register)
		}
	}
     packet, err := packetBuilder.CreatePacket(payloadBuffer.String(), true)
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
		fmt.Printf("Failed to read directory: {%s}\n", err)
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

