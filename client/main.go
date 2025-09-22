package main

import (
	"fmt"
	"os"

	"encoding/csv"

	"malasian_coffe/packet"
)

func createPackagesFrom(dir string, dirID int) (packet.Packet) {
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

	}
}

func sendPacket(packet packet.Packet, addr string) {
	
}

func main() {
	// Directory with all dataset subdirectories
	dataset_directory := os.Args[1]
	addr := os.Args[2]
	entries, err := os.ReadDir(dataset_directory)
	if err != nil {
		fmt.Printf("Failed to read directory: {}\n", err)
		panic("")
	}

	dirID := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subDirPath := dataset_directory + entry.Name()
		packet := createPackagesFrom(subDirPath, dirID)
		sendPacket(packet, addr)
		dirID += 1
	}
	fmt.Println("hello world")
}

