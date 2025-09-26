# TODO(fabri): emprolijar
build: build-server build-client

DATADIR               ?=    ../dataset/
GATEWAY_DIR           ?=    "localhost:9090"
LISTEN_DIR            ?=    "localhost:9091"
build-client:
	cd client; go run main.go ${DATADIR} ${GATEWAY_DIR} ${LISTEN_DIR}

build-server:
	cd system; go run main.go

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test-protocol:
	GOCACHE=off cd protocol/ ; go test -v ./...

test: test-server test-protocol

lint:
	./.github/scripts/check_go_version.sh

download-dataset:
	curl -C - -L https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506 -o dataset/dataset.zip
	unzip -n dataset/dataset.zip -d dataset/
# Delete unused dataset files
	rm -rf dataset/vouchers
	rm -rf dataset/payment_methods
