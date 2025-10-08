def header():
    return """name: tp1
services:
"""

def networks():
    return """
networks:
  testing_net:
    driver: bridge
"""

def commons():
    return """  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:4.1.4-management
    ports:
      - "5673:5672"
      - "15673:15672"
    networks:
      - testing_net
    healthcheck:
      test: ["CMD", "rabbitmqctl", "status"]
      interval: 5s
      retries: 5

  server:
    container_name: server
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./server_bin 0.0.0.0:9091 rabbitmq:5672
    volumes:
      - ./bin/server:/app/server_bin
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy

  gateway:
    container_name: gateway
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/gateway 0.0.0.0:9090 server:9091
    volumes:
      - ./bin/gateway:/app/bin/gateway
    networks:
      - testing_net
    ports:
      - 9090:9090
    depends_on:
      - server
"""

def filter_transactions_block(n):
    return f"""
  filter_transactions{n}:
    container_name: filter_transactions{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 transactions
    volumes:
      - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
      - testing_net
    depends_on:
      - server
"""

def filter_transaction_items_block(n):
    return f"""
  filter_transaction_items{n}:
    container_name: filter_transaction_items{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 transaction_items
    volumes:
        - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
        - testing_net
    depends_on:
        - server
"""

def concat_block(n):
    return f"""
  concat:
    container_name: concat
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/concat rabbitmq:5672
    volumes:
      - ./bin/concat:/app/bin/concat
    networks:
      - testing_net
    depends_on:
      - server
      - rabbitmq
"""

def sender_block(n, query):
    return f"""
  sender{n}:
    container_name: sender{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/sender rabbitmq:5672 Query{query}
    volumes:
      - ./bin/sender:/app/bin/sender
    networks:
      - testing_net
    depends_on:
      - server
      - concat
    extra_hosts:
      - "host.docker.internal:host-gateway"
"""

def read_config_file():
    with open("compose.config", "r") as file:
        lines = file.readlines()
    configs = {}
    for line in lines:
        if line.strip() == "" or line.startswith("#"):
          continue
        key, value = line.strip().split("=", 1)
        configs[key] = int(value)
    return configs

def main():
    configs = read_config_file()
    print(f"Configuration values: {configs}")

    output_file = "docker-compose-gen.yml"        
    with open(output_file, 'w') as file:
        file.write(header())
        file.write(commons())
        file.writelines(filter_transactions_block(i) for i in range(1, configs.get("filter-transactions", 0) + 1))
        file.writelines(filter_transaction_items_block(i) for i in range(1, configs.get("filter-transaction-items", 0) + 1))
        file.writelines(concat_block(i) for i in range(1, configs.get("concat1", 0) + 1))
        file.writelines(sender_block(i, 1) for i in range(1, configs.get("sender1", 0) + 1))

        
        file.write(networks())

if __name__ == "__main__":
    main()