package filter_mapper

import (
	"fmt"
	"malasian_coffe/packet"
	"testing"
)

func TestFilterMapperQuery1(t *testing.T) {
	// Mock data for testing
	data := []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54")
	fmt.Println("Data to be processed: \n", string(data))
	// Create a Packet instance
	pkt := packet.Packet{
		Payload: data,
	}

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction1,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery2a(t *testing.T) {
	// Mock data for testing
	data := []byte("b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\neab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\nc672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24")
	fmt.Println("Data to be processed: \n", string(data))
	// Create a Packet instance
	pkt := packet.Packet{
		Payload: data,
	}

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction2a,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("8,1,2025-05-01 10:51:41\n3,3,2024-07-01 10:53:42\n"),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery2b(t *testing.T) {
	// Mock data for testing
	data := []byte("b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\neab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\nc672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24")
	fmt.Println("Data to be processed: \n", string(data))
	// Create a Packet instance
	pkt := packet.Packet{
		Payload: data,
	}

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction2b,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("8,10.0,2025-05-01 10:51:41\n3,24.0,2024-07-01 10:53:42\n"),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery3Store(t *testing.T) {
	// Mock data for testing
	data := []byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n2,G Coffee @ Kondominium Putra,Jln Yew 6X,63826,Kondominium Putra,Selangor Darul Ehsan,2.959571,101.51772")
	fmt.Println("Data to be processed: \n", string(data))
	// Create a Packet instance
	pkt := packet.Packet{
		Payload: data,
	}

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction3Store,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("1,G Coffee @ USJ 89q\n2,G Coffee @ Kondominium Putra\n"),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}
