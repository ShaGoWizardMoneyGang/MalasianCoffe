package filter_mapper

import (
	"malasian_coffe/packet"
	"testing"
)

func TestFilterByYear(t *testing.T) {
	// Mock data for testing
	data := []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54"}

	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, data)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "yearFilter")

	// Validate the result
	expected := "2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n"

	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery1(t *testing.T) {
	// Mock data for testing
	transactionsRaw := []string{"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 07:00:00\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
		"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 07:02:21\n" +
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54"}
	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionsRaw)
	worker := FilterMapper{}

	result := worker.Process(pkt[0], "query1YearAndAmount")

	expected := "2ae6d188-76c2-4095-b861-ab97d3cd9312,38.0\n7d0a474d-62f4-442a-96b6-a5df2bda8832,33.0\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v, expected %+v", result[0].GetPayload(), expected)
	}
}

func TestFilterMapperQuery2a(t *testing.T) {
	transactionItemsRaw := []string{
		"b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\n" +
			"eab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\n" +
			"c672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24",
	}
	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionItemsRaw)
	worker := FilterMapper{}

	result := worker.Process(pkt[0], "query2aYearAndQuantity")
	expected := "8,1,2025-05-01 10:51:41\n3,3,2024-07-01 10:53:42\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result[0].GetPayload())
	}
}

func TestFilterMapperQuery2b(t *testing.T) {
	transactionItemsRaw := []string{
		"b8a05324-c892-4e1f-a4b8-c78ec3884847,8,1,10.0,10.0,2025-05-01 10:51:41\n" +
			"eab08b4e-fee8-4bf9-9a98-ce1c1d704111,3,3,8.0,24.0,2024-07-01 10:53:42\n" +
			"c672d808-733a-4562-835c-b278eda590d7,8,1,10.0,10.0,2023-12-01 11:42:24",
	}

	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionItemsRaw)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "query2bYearAndSubtotal")

	expected := "8,10.0,2025-05-01 10:51:41\n3,24.0,2024-07-01 10:53:42\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery3Store(t *testing.T) {
	// Mock data for testing
	storesRaw := []string{
		"1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n" +
			"2,G Coffee @ Kondominium Putra,Jln Yew 6X,63826,Kondominium Putra,Selangor Darul Ehsan,2.959571,101.51772",
	}

	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, storesRaw)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "query3MapStoreIdAndName")

	expected := "1,G Coffee @ USJ 89q\n2,G Coffee @ Kondominium Putra\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery3Transactions(t *testing.T) {
	transactionsRaw := []string{
		"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2024-07-01 23:01:00\n" +
			"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
			"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 05:30:21\n" +
			"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	}
	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionsRaw)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "query3Transactions")

	expected := "4,38.0,2024-07-01 23:01:00\n7,33.0,2025-07-01 07:00:02\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result)
	}
}

func TestFilterMapperQuery4Transactions(t *testing.T) {
	// Mock data for testing
	transactionsRaw := []string{
		"2e0b6369-f809-4de3-a2b5-eb932efe2f7a,1,5,,94144.0,40.0,0.0,40.0,2025-03-01 07:00:04\n" +
			"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,331213.0,33.0,0.0,33.0,2025-07-01 07:00:02\n" +
			"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	}

	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, transactionsRaw)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "query4Transactions")

	expected := "2e0b6369-f809-4de3-a2b5-eb932efe2f7a,1,94144.0\n" +
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,331213.0\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result[0].GetPayload())
	}
}

func TestFilterMapperQuery4Store(t *testing.T) {
	//es trivial porque hace lo mismo que el de query 3, hay que reutilizar la función y/o los resultados. Ojo.
}

func TestFilterMapperQuery4UsersBirthdates(t *testing.T) {
	usersRaw := []string{
		"9,female,1984-08-15,2023-07-01 12:33:40\n" +
			"8,female,2006-06-16,2023-07-01 12:32:21\n",
	}
	pkt_empty := packet.Packet{}
	pkt := packet.ChangePayload(pkt_empty, usersRaw)
	worker := FilterMapper{}
	result := worker.Process(pkt[0], "query4UsersBirthdates")

	// Validate the result
	expected := "9,1984-08-15\n8,2006-06-16\n"
	if result[0].GetPayload() != expected {
		t.Fatalf("unexpected result: got %+v", result[0].GetPayload())
	}
}
