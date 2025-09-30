#============================== Run directives =================================

DATADIR                ?=    ../dataset/
GATEWAY_ADDR           ?=    "localhost:9090"
CLIENT_LISTEN_ADDR     ?=    "localhost:9093"
run-client:
	cd client; go run client.go ${DATADIR} ${GATEWAY_ADDR} ${CLIENT_LISTEN_ADDR}

RABBIT_ADDR           ?=    "localhost:5672"
LISTEN_ADDR            ?=    "localhost:9092"
run-gateway:
	cd gateway; go run gateway.go ${GATEWAY_ADDR} ${LISTEN_ADDR}

run-server:
	cd system; go run system.go ${LISTEN_ADDR} ${RABBIT_ADDR}

run-filter:
	cd system/filter_mapper; go run filter_mapper.go ${RABBIT_ADDR}

run-concat: 
	cd system/concat; go run concat.go ${RABBIT_ADDR}

# RABBIT_ADDR           ?=    "amqp://guest:guest@localhost:5672/"
run-sender:
	cd system/sender; go run sender.go ${RABBIT_ADDR}
#============================== Build directives ===============================

current_dir = $(shell pwd)

build: build-server build-client build-gateway build-filter build-concat build-sender
build-client:
	cd client; go build -o ${current_dir}/bin/client

build-gateway:
	cd gateway; go build -o ${current_dir}/bin/gateway

build-server:
	cd system; go build -o ${current_dir}/bin/server

build-filter:
	cd system/filter_mapper; go build -o ${current_dir}/bin/filter_mapper

build-concat:
	cd system/concat; go build -o ${current_dir}/bin/concat

build-sender:
	cd system/sender; go build -o ${current_dir}/bin/sender

#=============================== Test directives ===============================

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test-packet:
	GOCACHE=off cd packet/ ; go test -v ./...

test-all:
	go test -v ./...

test: test-server test-packet test-all lint

lint:
	./.github/scripts/check_go_version.sh
	./.github/scripts/check_invariantes.sh

#============================== Misc directives ===============================
download-dataset:
	curl -C - -L https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506 -o dataset/dataset.zip
	unzip -n dataset/dataset.zip -d dataset/
# Delete unused dataset files
	rm -rf dataset/vouchers
	rm -rf dataset/payment_methods

rabbit-gui:
	xdg-open http://localhost:15672
