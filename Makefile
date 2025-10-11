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
distro = $(shell cat /etc/os-release | grep -w NAME | sed 's/NAME=//g' )


BINDIR                  =    ${current_dir}/bin
DATADIR                ?=    ${current_dir}/dataset/
OUTDIR                 ?=    ${current_dir}/out/
GATEWAY_ADDR           ?=    "localhost:9090"
CLIENT_LISTEN_ADDR     ?=    "0.0.0.0:9093"
RABBIT_ADDR            ?=    "localhost:5672"
SERVER_ADDR            ?=    "localhost:9092"
SENDER_CONN_ADDR 	   ?=    "host.docker.internal:9093"

# El nombre de la funcion a ejecutar
RUN_FUNCTION           ?=    ""
run-server: build-server
	${BINDIR}/server ${SERVER_ADDR} ${RABBIT_ADDR}

run-client: build-client clean-out
	${BINDIR}/client ${DATADIR} ${OUTDIR} ${GATEWAY_ADDR} ${CLIENT_LISTEN_ADDR} ${SENDER_CONN_ADDR}
	@echo "Cliente finalizo, comparar resultados con 'make test-outputs-reduced'"

run-gateway: build-gateway
	${BINDIR}/gateway ${GATEWAY_ADDR} ${SERVER_ADDR} ${RUN_FUNCTION}

run-filter: build-filter
	${BINDIR}/filter_mapper ${RABBIT_ADDR} ${RUN_FUNCTION}

run-concat: build-concat
	${BINDIR}/concat ${RABBIT_ADDR} ${RUN_FUNCTION}

run-sender: build-sender
	${BINDIR}/sender ${RABBIT_ADDR} ${RUN_FUNCTION}

run-counter: build-counter
	${BINDIR}/counter ${RABBIT_ADDR} ${RUN_FUNCTION}

run-joiner: build-joiner
	${BINDIR}/joiner ${RABBIT_ADDR} ${RUN_FUNCTION}

run-partial-aggregator: build-partial-aggregator
	${BINDIR}/partial_aggregator ${RABBIT_ADDR} ${RUN_FUNCTION}

run-global-aggregator: build-global-aggregator
	${BINDIR}/global_aggregator ${RABBIT_ADDR} ${RUN_FUNCTION}

#============================== Build directives ===============================
# Poner en order
build: build-server build-client build-gateway build-filter build-concat build-sender build-counter build-joiner build-partial-aggregator build-global-aggregator build-test-output-query-4
build-server:
	cd system; go build -o ${BINDIR}/server

build-client:
	cd client; go build -o ${BINDIR}/client

build-gateway:
	cd gateway; go build -o ${BINDIR}/gateway

build-filter:
	cd system/filter_mapper; go build -o ${BINDIR}/filter_mapper

build-concat:
	cd system/concat; go build -o ${BINDIR}/concat

build-sender:
	cd system/sender; go build -o ${BINDIR}/sender

build-counter:
	cd system/counter; go build -o ${BINDIR}/counter

build-joiner:
	cd system/joiner; go build -o ${BINDIR}/joiner

build-partial-aggregator:
	cd system/partial_aggregator; go build -o ${BINDIR}/partial_aggregator

build-global-aggregator:
	cd system/global_aggregator; go build -o ${BINDIR}/global_aggregator

build-test-output-query-4:
	go build -o ${current_dir}/bin/test_output_query_4 ./test_output_query4/test_output_query4.go
#=============================== Test directives ===============================

test-server:
	GOCACHE=off cd system/ ; go test -v ./...

test-packet:
	GOCACHE=off cd packets/ ; go test -v ./...

test-all:
	go test -v ./...

test: test-server test-packet test-all lint

lint:
	./.github/scripts/check_go_version.sh
	./.github/scripts/check_invariantes.sh

test-outputs-reduced:
	bash scripts/test_outputs.sh RED

#=================================== Docker ====================================
CONFIG                  ?= MuchosSinEstado
generate-config:
	bash scripts/generate-config.sh ${CONFIG}

generate-compose:
	python3 scripts/generar_compose.py

docker-multi: docker-down clean-out build generate-compose
	docker compose -f docker-compose-gen.yml up -d
	@echo "Docker levantado, usar 'make docker-wait ; make test-outputs-reduced'"

docker-wait:
	bash scripts/wait_for_clients.sh

docker-down:
	docker compose -f docker-compose-gen.yml down -v --remove-orphans

docker-logs:
	docker compose -f docker-compose-gen.yml logs

docker-ci:
	docker compose -f docker-compose-ci.yml up -d

#============================== Misc directives ===============================
clean-out:
	find out/ ! -name '.gitignore' ! -name 'out' -type d -exec rm -irf {} +

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

restart-rabbit:
ifeq ($(distro),Gentoo)
	sudo rc-service rabbitmq restart
else
	sudo systemctl restart rabbitmq-server
endif
