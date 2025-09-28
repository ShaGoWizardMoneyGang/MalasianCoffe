#============================== Run directives =================================

DATADIR                ?=    ../dataset/
GATEWAY_ADDR           ?=    "localhost:9090"
LISTEN_ADDR            ?=    "localhost:9091"
run-client:
	cd client; go run main.go ${DATADIR} ${GATEWAY_ADDR} ${LISTEN_ADDR}

RABBIT_ADDR           ?=    "localhost:9092"
run-gateway:
	cd gateway; go run gateway.go ${GATEWAY_ADDR} ${RABBIT_ADDR}

run-server:
	cd system; go run main.go ${RABBIT_ADDR}

#============================== Build directives ===============================

current_dir = $(shell pwd)

build: build-server build-client build-gateway
build-client:
	cd client; go build -o ${current_dir}/bin/client

build-gateway:
	cd gateway; go build -o ${current_dir}/bin/gateway

build-server:
	cd system; go build -o ${current_dir}/bin/server

#=============================== Test directives ===============================

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test-packet:
	GOCACHE=off cd packet/ ; go test -v ./...

test: test-server test-packet

lint:
	./.github/scripts/check_go_version.sh

#============================== Misc directives ===============================
download-dataset:
	curl -C - -L https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506 -o dataset/dataset.zip
	unzip -n dataset/dataset.zip -d dataset/
# Delete unused dataset files
	rm -rf dataset/vouchers
	rm -rf dataset/payment_methods
