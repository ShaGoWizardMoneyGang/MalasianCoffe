import os
import time

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
    entrypoint: ./bin/server_bin 0.0.0.0:9091 rabbitmq:5672
    volumes:
      - ./bin/server:/app/bin/server_bin
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
    entrypoint: ./bin/filter_mapper rabbitmq:5672 transactions 0
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
    entrypoint: ./bin/filter_mapper rabbitmq:5672 transaction_items 0
    volumes:
        - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
        - testing_net
    depends_on:
        - server
"""

def filter_users_block(n, queueAmount4):
    return f"""
  filter_users{n}:
    container_name: filter_users{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 users 1 queue4:{queueAmount4}
    volumes:
      - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
      - testing_net
    depends_on:
      - server
"""

def filter_stores_block(n, queueAmount3, queueAmount4):
    return f"""
  filter_stores{n}:
    container_name: filter_stores{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 stores 2 queue3:{queueAmount3} queue4:{queueAmount4}
    volumes:
      - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
      - testing_net
    depends_on:
      - server
"""

def filter_menu_items_block(n):
    return f"""
  filter_menu_items{n}:
    container_name: filter_menu_items{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 menu_items 0
    volumes:
      - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
      - testing_net
    depends_on:
      - server
"""

def concat_block(n):
    return f"""
  concat_{n}:
    container_name: concat_{n}
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
  sender{query}_{n}:
    container_name: sender{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/sender rabbitmq:5672 Query{query}
    volumes:
      - ./bin/sender:/app/bin/sender
    networks:
      - testing_net
    depends_on:
      - server
    extra_hosts:
      - "host.docker.internal:host-gateway"
"""

def counter_block(n, query, queueAmount):
    return f"""
  counter{query}_{n}:
    container_name: counter{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/counter rabbitmq:5672 Query{query} 1 queue:{queueAmount}
    volumes:
      - ./bin/counter:/app/bin/counter
    networks:
      - testing_net
    depends_on:
      - server
"""

def global_aggregator_block(n, query, queueAmount):
    routing_key = int(n) - 1
    return f"""
  global_aggregator{query}_{n}:
    container_name: global_aggregator{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/global_aggregator rabbitmq:5672 Query{query} {routing_key} 1 queue:{queueAmount}
    volumes:
      - ./bin/global_aggregator:/app/bin/global_aggregator
    networks:
      - testing_net
    depends_on:
      - server
"""

def joiner_block(n, query):
    routing_key = int(n) - 1

    return f"""
  joiner{query}_{n}:
    container_name: joiner{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/joiner rabbitmq:5672 Query{query} {routing_key}
    volumes:
      - ./bin/joiner:/app/bin/joiner
    networks:
      - testing_net
    depends_on:
      - server
"""

def partial_aggregator_block(n, query):
    return f"""
  partial_aggregator{query}_{n}:
    container_name: partial_aggregator{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/partial_aggregator rabbitmq:5672 Query{query}
    volumes:
      - ./bin/partial_aggregator:/app/bin/partial_aggregator
    networks:
      - testing_net
    depends_on:
      - server
"""

def client(n):
    # Creo un directorio en out para que no sea creado por root
    os.mkdir(f"out/client{n}")

    port_number = 9093 + n

    return f"""
  client{n}:
    container_name: client{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/client ./dataset/ ./out/client{n}/ gateway:9090 client{n}:{port_number}
    volumes:
      - ./bin/client:/app/bin/client
      - ./dataset:/app/dataset
      - ./out/client{n}:/app/out/client{n}
    depends_on:
      - gateway
    networks:
      - testing_net
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

def display_config_table(configs):
    longest_var = 0
    for key, _ in configs.items():
        if len(key) > longest_var:
            longest_var = len(key)

    # NOTA: Le deseo lo mejor a quien sea que tenga que debugear esto.
    # Lo hice porque queria descansar, perdon.
    padding_to_bar = lambda word: longest_var - len(word) - 1 + 4
    print("┌" + "─" * (longest_var + 3) + "┬" + "─" * (len("Amount"))  + "┐")
    print("│" + "Worker" + " " * padding_to_bar("Worker") + "│" + "Amount" + "│")
    for key, value in configs.items():
        print("│" + key + " " * (padding_to_bar(key)) + "│" + " " * 2 + str(value) + " " * (len("Amount") - len(str(value)) - 2) + "│")
    print("└" + "─" * (longest_var + 3) + "┴" + "─" * (len("Amount"))  + "┘")

    time.sleep(1)

def main():
    configs = read_config_file()
    display_config_table(configs)

    output_file = "docker-compose-gen.yml"        
    with open(output_file, 'w') as file:
        file.write(header())
        file.write(commons())
        file.writelines(filter_transactions_block(i) for i in range(1, configs.get("filter-transactions", 0) + 1))
        file.writelines(filter_transaction_items_block(i) for i in range(1, configs.get("filter-transaction-items", 0) + 1))
        file.writelines(filter_users_block(i, configs["joiner4"]) for i in range(1, configs.get("filter-users", 0) + 1))
        file.writelines(filter_stores_block(i, configs["joiner3"], configs["joiner4"]) for i in range(1, configs.get("filter-stores", 0) + 1))
        file.writelines(filter_menu_items_block(i) for i in range(1, configs.get("filter-menu-items", 0) + 1))
        

        
        file.writelines(concat_block(i) for i in range(1, configs.get("concat1", 0) + 1))
        
        file.writelines(sender_block(i, "1") for i in range(1, configs.get("sender1", 0) + 1))
        file.writelines(sender_block(i, "2a") for i in range(1, configs.get("sender2a", 0) + 1))
        file.writelines(sender_block(i, "2b") for i in range(1, configs.get("sender2b", 0) + 1))
        file.writelines(sender_block(i, "3") for i in range(1, configs.get("sender3", 0) + 1))
        file.writelines(sender_block(i, "4") for i in range(1, configs.get("sender4", 0) + 1))
        
        file.writelines(counter_block(i, "2a", configs["global-aggregator2a"]) for i in range(1, configs.get("counter2a", 0) + 1))
        file.writelines(counter_block(i, "2b", configs["global-aggregator2b"]) for i in range(1, configs.get("counter2b", 0) + 1))
        file.writelines(counter_block(i, "4", configs["global-aggregator4"]) for i in range(1, configs.get("counter4", 0) + 1))

        file.writelines(global_aggregator_block(i, "2a", configs["joiner2a"]) for i in range(1, configs.get("global-aggregator2a", 0) + 1))
        file.writelines(global_aggregator_block(i, "2b", configs["joiner2b"]) for i in range(1, configs.get("global-aggregator2b", 0) + 1))
        file.writelines(global_aggregator_block(i, "3", configs["joiner3"]) for i in range(1, configs.get("global-aggregator3", 0) + 1))
        file.writelines(global_aggregator_block(i, "4", configs["joiner4"]) for i in range(1, configs.get("global-aggregator4", 0) + 1))

        file.writelines(joiner_block(i, "2a") for i in range(1, configs.get("joiner2a", 0) + 1))
        file.writelines(joiner_block(i, "2b") for i in range(1, configs.get("joiner2b", 0) + 1))
        file.writelines(joiner_block(i, "3") for i in range(1, configs.get("joiner3", 0) + 1))
        file.writelines(joiner_block(i, "4") for i in range(1, configs.get("joiner4", 0) + 1))

        file.writelines(partial_aggregator_block(i, "3") for i in range(1, configs.get("partial-aggregator3", 0) + 1))
        file.writelines(partial_aggregator_block(i, "4") for i in range(1, configs.get("partial-aggregator4", 0) + 1))

        file.writelines(client(i) for i in range(1, configs.get("cliente", 0) + 1))

        file.write(networks())

if __name__ == "__main__":
    main()
