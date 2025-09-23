package filter_mapper

import (
	"fmt"
	"malasian_coffe/packet"
	"testing"
)

func TestFilterMapper(t *testing.T) {
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
