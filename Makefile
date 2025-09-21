# TODO(fabri): emprolijar
build: build-server build-client

build-client:
	cd client; go run main.go

build-server:
	cd system; go run main.go

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test: test-server
