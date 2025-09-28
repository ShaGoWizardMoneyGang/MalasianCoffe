package main

import (
	// "encoding/binary"
	"bytes"
	"fmt"
	"net"

	"os"

	"bufio"

	"malasian_coffe/packet"
	"malasian_coffe/protocol"
	"malasian_coffe/utils/network"
)

const (
	// Max batch size es 8192 simplemente porque es el valor default de BUFSIZ en glibc:
	// https://sourceware.org/git/?p=glibc.git;a=blob;f=libio/stdio.h;h=e0e70945fab175fafcb0c8bbae96ad7eebe3df5a;hb=HEAD#l100
	// Ademas, en el tp0 el maximo era 8000, el cual es parecido en tamano
	MAX_BATCH_SIZE int  = 8192
)


func createPackagesFrom(dir string, dirID uint, session_ID string, listen_addr string, send_addr net.Conn) (error) {
	packetBuilder := packet.NewPacketBuilder(dirID, session_ID, listen_addr, send_addr)
	// var payloadBuffer strings.Builder
	// payloadBuffer.Grow(MAX_BATCH_SIZE)

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
			return fmt.Errorf("Couldn't open csv file in dir {%s}, because of {%s}", dir, err)
		}
		csv_reader := bufio.NewScanner(csv_file);
		{
			// Skip first line which holds column names
			csv_reader.Scan()
		}

		for csv_reader.Scan() {
			register := csv_reader.Text() + "\n"

			err = packetBuilder.Send(register)
			if err != nil {
				return err
			}
		}
	}

	err = packetBuilder.End()
	if err != nil {
		return err
	}

	return nil
}

func main() {
	// Directory with all dataset subdirectories
	dataset_directory := os.Args[1]

	addr := os.Args[2]
	conn, err := net.Dial("tcp", addr)

	listen_addr := os.Args[3]

	string_b, err := network.ReceiveFromNetwork(conn)
	if err != nil {
		panic(err)
	}
	string_reader := bytes.NewReader(string_b)
	session_id, err := protocol.DeserializeString(string_reader)
	if err != nil {
		panic(err)
	}
	println(session_id)

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
		err := createPackagesFrom(subDirPath, dirID, session_id, listen_addr, conn)
		if err != nil {
			 panic(err)
		}

		dirID += 1
	}
	fmt.Println("I am the client")
}

