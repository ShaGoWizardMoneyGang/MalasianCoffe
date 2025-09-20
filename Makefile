# TODO(fabri): emprolijar
build-server:
	cd system; go run main.go

test:
	cd system/ ; go test ./...
