package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"malasian_coffe/packets/packet"
	packetanswer "malasian_coffe/packets/packet_answer"
	"malasian_coffe/protocol"
	"malasian_coffe/utils/network"

	"github.com/fatih/color"
)

func createPackagesFrom(dir string, session_ID string, listen_addr string, send_addr net.Conn) error {
	directory_name := filepath.Base(dir)
	packetBuilder := packet.NewPacketBuilder(directory_name, session_ID, listen_addr, send_addr)

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
		csv_reader := bufio.NewScanner(csv_file)
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
	dataset_directory := os.Args[1]
	out_dir := os.Args[2]
	gateway_addr := os.Args[3]
	listen_addr := os.Args[4]

	conn, err := net.Dial("tcp", gateway_addr)

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

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subDirPath := dataset_directory + entry.Name()
		err := createPackagesFrom(subDirPath, session_id, listen_addr, conn)
		if err != nil {
			panic(err)
		}

	}

	fmt.Println("All dataset sent, now waiting for replies")

	error := waitForAnswers(listen_addr, out_dir)
	if error != nil {
		panic(error)
	}

}

type receive_answer struct {
	query_name string
	received   bool
}

type received_answers struct {
	received []receive_answer
}

func new_received_answers() received_answers {
	buffer := make([]receive_answer, 6)
	buffer[0] = receive_answer{
		query_name: "Query1",
		received:   false,
	}
	buffer[1] = receive_answer{
		query_name: "Query2a",
		received:   false,
	}
	buffer[2] = receive_answer{
		query_name: "Query2b",
		received:   false,
	}
	buffer[3] = receive_answer{
		query_name: "Query3",
		received:   false,
	}
	buffer[4] = receive_answer{
		query_name: "Query4",
		received:   false,
	}
	buffer[5] = receive_answer{
		query_name: "Query5",
		received:   false,
	}

	received_answers := received_answers{
		received: buffer,
	}

	return received_answers
}

func (ra *received_answers) addAnswer(pkt packetanswer.PacketAnswer) {
	var index int
	switch pkt.GetQuery() {
	case "Query1":
		index = 0
	case "Query2a":
		index = 1
	case "Query2b":
		index = 2
	case "Query3":
		index = 3
	case "Query4":
		index = 4
	case "Query5":
		index = 5
	default:
		panic(fmt.Sprintf("Unknown query: %s", pkt.GetQuery()))
	}

	ra.received[index].received = true

	// TODO: Escribir texto a archivo
	fmt.Printf("Recibi paquete respuesta de la %s: \n", pkt.GetQuery())
}

func (ra *received_answers) display() {
	// Taken from: https://stackoverflow.com/a/22892171/13683575
	fmt.Print("\033[H\033[2J")
	for _, answer := range ra.received {
		if answer.received {
			color.Green("%s received", answer.query_name)
		} else {
			color.Red("%s not received", answer.query_name)
		}
	}
}

func waitForAnswers(listen_addr string, out_dir string) error {
	err := os.MkdirAll(out_dir, 0777)
	if err != nil {
		panic(fmt.Errorf("Failed to create directory %s", out_dir))
	}
	received_answers := new_received_answers()

	list, err := net.Listen("tcp", listen_addr)
	if err != nil {
		panic(fmt.Errorf("Failed to create listener %w", err))
	}
	for {
		received_answers.display()
		// NOTE: Esto supone que la respuesta te llega en un solo Packet
		conn, err := list.Accept()
		if err != nil {
			return err
		}

		packet_answer_b, err := network.ReceiveFromNetwork(conn)
		if err != nil {
			return fmt.Errorf("Failed to receive packet from %s because of %s", conn.LocalAddr().String(), err)
		}

		packet_answer_reader := bytes.NewReader(packet_answer_b)
		packet_answer, err := packetanswer.DeserializePackageAnswer(packet_answer_reader)
		if err != nil {
			return fmt.Errorf("Failed to deserialize packet because of %s", err)
		}

		write_to_file(packet_answer, out_dir)
		received_answers.addAnswer(packet_answer)
	}
}

func write_to_file(pkt packetanswer.PacketAnswer, out_dir string) error {
	result := []byte(pkt.GetPayload())

	query := pkt.GetQuery()

	out_file := out_dir + query + ".csv"

	err := os.WriteFile(out_file, result, 0666)
	if err != nil {
		return err
	}

	return nil
}
