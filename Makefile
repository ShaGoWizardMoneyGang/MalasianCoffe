# Calling convention, prioridades (si aplica)
# 1. Address propia
# 2. Input
# 3. Output
# 4. Address del servidor
# 5. Address de rabbit
# 6. Nombre de la funcion
# Ejemplo:
# make run-filter RUN_FUNCTION="query1YearAndAmount"
#============================== Run directives =================================

current_dir = $(shell pwd)

DATADIR                ?=    ${current_dir}/dataset/
OUTDIR                 ?=    ${current_dir}/out/
GATEWAY_ADDR           ?=    "localhost:9090"
CLIENT_LISTEN_ADDR     ?=    "localhost:9093"
run-client:
	cd client; go run client.go ${DATADIR} ${OUTDIR} ${GATEWAY_ADDR} ${CLIENT_LISTEN_ADDR}

RABBIT_ADDR           ?=    "localhost:5672"
SERVER_ADDR           ?=    "localhost:9092"

# El nombre de la funcion a ejecutar
RUN_FUNCTION          ?=    ""
run-gateway:
	cd gateway; go run gateway.go ${GATEWAY_ADDR} ${SERVER_ADDR} ${RUN_FUNCTION}

run-server:
	cd system; go run system.go ${SERVER_ADDR} ${RABBIT_ADDR}

run-filter:
	cd system/filter_mapper; go run filter_mapper.go ${RABBIT_ADDR} ${RUN_FUNCTION}

run-concat:
	cd system/concat; go run concat.go ${RABBIT_ADDR} ${RUN_FUNCTION}

run-sender:
	cd system/sender; go run sender.go ${RABBIT_ADDR} ${RUN_FUNCTION}
#============================== Build directives ===============================
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

download-reduced-dataset: download-dataset
	find dataset/transaction_items -type f ! \( -name '*202401*' -o -name '*202501*' \) -exec rm {} +
	find dataset/transactions -type f ! \( -name '*202401*' -o -name '*202501*' \) -exec rm {} +

rabbit-gui:
	xdg-open http://localhost:15672
