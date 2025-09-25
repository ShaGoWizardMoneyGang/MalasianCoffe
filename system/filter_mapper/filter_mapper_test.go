package filter_mapper

import (
	"fmt"
	"malasian_coffe/packet"
	"testing"
)

func TestFilterByYear(t *testing.T) {
	// Mock data for testing
	data := []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54")
	fmt.Println("Data to be processed: \n", string(data))
	// Create a Packet instance
	pkt := packet.Packet{
		Payload: data,
	}

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterByYearCommon,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte(
			"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
				"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n",
		),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}
func TestFilterMapperQuery1(t *testing.T) {
	// Mock data for testing
	transactionsRaw := []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54")
	fmt.Println("Data to be processed: \n", string(transactionsRaw))
	// Create a Packet instance
	transactionsRawPkt := packet.Packet{
		Payload: transactionsRaw,
	}
	worker1 := FilterMapper{
		Function: filterByYearCommon,
	}

	transactionsByYearPkt := worker1.Process(transactionsRawPkt)
	// Create a new worker using the filter mapper options
	filterByAmount := FilterMapper{
		Function: filterFunction1,
	}

	// Process the packet using the worker
	transactions2024_2025_amountGreaterThan15 := string(filterByAmount.Process(transactionsByYearPkt).Payload)
	fmt.Println("Processed result: \n", transactions2024_2025_amountGreaterThan15)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"),
	}
	if transactions2024_2025_amountGreaterThan15 != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", transactions2024_2025_amountGreaterThan15)
	}
}

func TestFilterMapperQuery2a(t *testing.T) {
	// Mock data for testing
	transactionItemsRaw := []byte(
		"b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\n" +
			"eab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\n" +
			"c672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24",
	)
	fmt.Println("Data to be processed: \n", string(transactionItemsRaw))

	// Create a Packet instance
	pkt := packet.Packet{
		Payload: transactionItemsRaw,
	}

	// Create a new worker using the filter mapper options
	filterByYear := FilterMapper{
		Function: filterFunction2a,
	}

	// Process the packet using the worker
	transactionItemsByYear := string(filterByYear.Process(pkt).Payload)
	fmt.Println("Processed result: \n", transactionItemsByYear)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte(
			"8,1,2025-05-01 10:51:41\n" +
				"3,3,2024-07-01 10:53:42\n",
		),
	}
	if transactionItemsByYear != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", transactionItemsByYear)
	}
}

func TestFilterMapperQuery2b(t *testing.T) {
	// Mock data for testing
	transactionItemsRaw := []byte(
		"b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\n" +
			"eab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\n" +
			"c672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24",
	)
	fmt.Println("Data to be processed: \n", string(transactionItemsRaw))

	// Create a Packet instance
	pkt := packet.Packet{
		Payload: transactionItemsRaw,
	}

	// Create a new worker using the filter mapper options
	filterByYear := FilterMapper{
		Function: filterFunction2b,
	}

	// Process the packet using the worker
	transactionItemsByYear := string(filterByYear.Process(pkt).Payload)
	fmt.Println("Processed result: \n", transactionItemsByYear)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte(
			"8,10.0,2025-05-01 10:51:41\n" +
				"3,24.0,2024-07-01 10:53:42\n",
		),
	}
	if transactionItemsByYear != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", transactionItemsByYear)
	}
}

func TestFilterMapperQuery3Store(t *testing.T) {
	// Mock data for testing
	storesRaw := []byte(
		"1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n" +
			"2,G Coffee @ Kondominium Putra,Jln Yew 6X,63826,Kondominium Putra,Selangor Darul Ehsan,2.959571,101.51772",
	)
	fmt.Println("Data to be processed: \n", string(storesRaw))
	// Create a Packet instance
	storesRawPkt := packet.Packet{
		Payload: storesRaw,
	}

	// Create a new worker using the filter mapper options
	mapperStoreIdName := FilterMapper{
		Function: filterFunction3Store,
	}

	// Process the packet using the worker
	result := string(mapperStoreIdName.Process(storesRawPkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte("1,G Coffee @ USJ 89q\n2,G Coffee @ Kondominium Putra\n"),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery3Transactions(t *testing.T) {
	// Mock data for testing
	transactionsRaw := []byte(
		"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
			"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
			"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 05:30:21\n" +
			"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	)
	fmt.Println("Data to be processed: \n", string(transactionsRaw))
	// Create a Packet instance
	transactionsRawPkt := packet.Packet{
		Payload: transactionsRaw,
	}
	worker1 := FilterMapper{
		Function: filterByYearCommon,
	}

	pkt := worker1.Process(transactionsRawPkt)

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction3Transactions,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte(
			"4,38.0,2024-07-01 23:01:00\n" +
				"7,33.0,2025-07-01 07:00:02\n",
		),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery4Transactions(t *testing.T) {
	// Mock data for testing
	transactionsRaw := []byte(
		"2e0b6369-f809-4de3-a2b5-eb932efe2f7a,1,5,,94144.0,40.0,0.0,40.0,2025-03-01 07:00:04\n" +
			"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,331213.0,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
			"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	)
	fmt.Println("Data to be processed: \n", string(transactionsRaw))
	// Create a Packet instance
	transactionsRawPkt := packet.Packet{
		Payload: transactionsRaw,
	}
	worker1 := FilterMapper{
		Function: filterByYearCommon,
	}

	pkt := worker1.Process(transactionsRawPkt)

	// Create a new worker using the filter mapper options
	worker := FilterMapper{
		Function: filterFunction4Transactions,
	}

	// Process the packet using the worker
	result := string(worker.Process(pkt).Payload)
	fmt.Println("Processed result: \n", result)

	// Validate the result
	expected := packet.Packet{
		Payload: []byte(
			"2e0b6369-f809-4de3-a2b5-eb932efe2f7a,1,94144.0\n" +
				"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,331213.0\n",
		),
	}
	if result != string(expected.Payload) {
		t.Fatalf("unexpected result: got %+v, expected %+v", result, string(expected.Payload))
	}
}
