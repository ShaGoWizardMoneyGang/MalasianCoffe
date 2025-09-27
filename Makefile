#============================== Build directive ===============================

# TODO(fabri): emprolijar
build: build-server build-client

DATADIR               ?=    ../dataset/
GATEWAY_ADDR           ?=    "localhost:9090"
LISTEN_ADDR            ?=    "localhost:9091"
build-client:
	cd client; go run main.go ${DATADIR} ${GATEWAY_ADDR} ${LISTEN_ADDR}

RABBIT_ADDR           ?=    "localhost:9092"
build-gateway:
	cd client; go run main.go ${DATADIR} ${RABBIT_ADDR} ${LISTEN_ADDR}

build-server:
	cd system; go run main.go

#=============================== Test directive ================================

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test-packet:
	GOCACHE=off cd packet/ ; go test -v ./...

test: test-server test-packet

lint:
	./.github/scripts/check_go_version.sh

download-dataset:
	curl -C - -L https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506 -o dataset/dataset.zip
	unzip -n dataset/dataset.zip -d dataset/
# Delete unused dataset files
	rm -rf dataset/vouchers
	rm -rf dataset/payment_methods
