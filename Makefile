# TODO(fabri): emprolijar
build-server:
	cd system; go run main.go

test:
	GOCACHE=off cd system/ ; go test -v ./...
