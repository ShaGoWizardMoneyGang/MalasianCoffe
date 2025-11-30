import os
import time
import sys
import random

def header():
    return """name: tp1
services:
"""

def networks(external: bool):
    network_name = """
  testing_net:
    """
    if external == True:
        network_name = """
  tp1_testing_net:
    """

    external_text = ""
    if external == True:
        external_text = "external: true"
    return f"""
networks:
  {network_name}
    {external_text}
    driver: bridge
"""
# Saco directamente los sheeps de los parámetros
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

def filter_transactions_block(n, queueAmount1):
    return f"""
  filter_transactions{n}:
    container_name: filter_transactions{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 transactions 1 queue1:{queueAmount1}
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

def filter_menu_items_block(n, queueAmount2a, queueAmount2b):
    return f"""
  filter_menu_items{n}:
    container_name: filter_menu_items{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/filter_mapper rabbitmq:5672 menu_items 2 queue2a:{queueAmount2a} queue2b:{queueAmount2b}
    volumes:
      - ./bin/filter_mapper:/app/bin/filter_mapper
    networks:
      - testing_net
    depends_on:
      - server
"""

def concat_block(n):
    name = f"concat_{n}"
    routing_key = int(n) - 1

    os.mkdir(f"packet_receiver/{name}")
    # os.mkdir(f"packet_receiver/{name}/metadata")
    # os.mkdir(f"packet_receiver/{name}/checkpoint")

    return f"""
  {name}:
    container_name: {name}
    image: worker:latest
    working_dir: /app
    entrypoint: bash -c "entrypoint.sh && su user -c '/app/bin/concat rabbitmq:5672 {routing_key}'"
    volumes:
      - ./bin/concat:/app/bin/concat
      - ./packet_receiver/{name}:/app/packet_receiver/
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
    name = f"joiner{query}_{n}"
    routing_key = int(n) - 1

    os.mkdir(f"packet_receiver/{name}")

    return f"""
  {name}:
    container_name: {name}
    image: worker:latest
    working_dir: /app
    entrypoint: bash -c "entrypoint.sh && su user -c '/app/bin/joiner rabbitmq:5672 Query{query} {routing_key}'"
    volumes:
      - ./bin/joiner:/app/bin/joiner
      - ./packet_receiver/{name}:/app/packet_receiver/
    networks:
      - testing_net
    depends_on:
      - server
"""

def partial_aggregator_block(n, query, queueAmount):
    routing_key = int(n) - 1
    return f"""
  partial_aggregator{query}_{n}:
    container_name: partial_aggregator{query}_{n}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/partial_aggregator rabbitmq:5672 Query{query} 1 queue:{queueAmount}
    volumes:
      - ./bin/partial_aggregator:/app/bin/partial_aggregator
    networks:
      - testing_net
    depends_on:
      - server
"""

# Devuelve la cantidad de watchdogs lideres y no lideres se necesita
def cant_watchdogs(n):
    if n == 0:
        return (0, 0)

    lider_cant = 1
    no_lider_cant = n - 1

    return (lider_cant, no_lider_cant)

def leader_watchdog_block(n):
    return f"""
  watchdog_{n}:
    container_name: watchdog_{n}
    hostname: watchdog_{n}
    image: watchdog:latest
    working_dir: /app
    entrypoint: ./bin/watchdog STARTER
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./sheeps.txt:/app/sheeps.txt
      - ./members.txt:/app/members.txt
    networks:
      - testing_net
    depends_on:
      - server
"""

def replica_watchdog_block(n):
    return f"""
  watchdog_{n}:
    container_name: watchdog_{n}
    hostname: watchdog_{n}
    image: watchdog:latest
    working_dir: /app
    entrypoint: ./bin/watchdog REPLICA
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./sheeps.txt:/app/sheeps.txt
      - ./members.txt:/app/members.txt
    networks:
      - testing_net
    depends_on:
      - watchdog_{n-1}
"""


def client(n, external):
    # Creo un directorio en out para que no sea creado por root

    port_number = 9093 + n
    suffix = ""
    if external:
        suffix = random.randint(1, 1000000)

    name = str(n) + str(suffix)
    os.mkdir(f"out/client{name}")

    depends = """
    depends_on:
      - gateway
    """
    if external:
        depends = ""

    network = """
    networks:
      - testing_net
    """
    if external:
        network = """
    networks:
      - tp1_testing_net
    """

    return f"""
  client{name}:
    container_name: client{name}
    image: ubuntu:24.04
    working_dir: /app
    entrypoint: ./bin/client ./dataset/ ./out/client{name}/ gateway:9090 client{name}:{port_number}
    volumes:
      - ./bin/client:/app/bin/client
      - ./dataset:/app/dataset
      - ./out/client{name}:/app/out/client{name}
    {depends}
    {network}
"""

def read_config_file(config_file_name):
    with open(config_file_name, "r") as file:
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
    external = False
    if len(sys.argv) > 1 and sys.argv[1] == "EXTERNAL":
        external = True

    config_file_name = "compose.config"
    if external:
        config_file_name = "compose-external.config"


    configs = read_config_file(config_file_name)
    display_config_table(configs)


    output_file = "docker-compose-gen.yml"
    if external:
        output_file = "docker-compose-gen-external.yml"

    #Guardo los servicios en una lista y después escribo todo juntito
    sheeps_list = []
    members_list = [] # aca guardo los nodos del anillo, los separo de la otra logixca

    with open(output_file, 'w') as file:
        file.write(header())
        if external == False:
            file.write(commons())

        lider_cant, no_lider_cant = cant_watchdogs(configs.get("watchdog", 0))

        for i in range(lider_cant):
            lider_id = i + 1
            file.writelines(leader_watchdog_block(lider_id))

        file.writelines(replica_watchdog_block(i) for i in range(2, no_lider_cant + 1))

        for i in range(1, configs.get("watchdog", 0) + 1):
            members_list.append(f"watchdog_{i}")
        for i in range(1, configs.get("watchdog", 0) + 1):
            sheeps_list.append(f"watchdog_{i}")

        file.writelines(filter_transactions_block(i, configs["concat1"]) for i in range(1, configs.get("filter-transactions", 0) + 1))
        for i in range(1, configs.get("filter-transactions", 0) + 1):
            sheeps_list.append(f"filter_transactions{i}")

        file.writelines(filter_transaction_items_block(i) for i in range(1, configs.get("filter-transaction-items", 0) + 1))
        for i in range(1, configs.get("filter-transaction-items", 0) + 1):
            sheeps_list.append(f"filter_transaction_items{i}")

        file.writelines(filter_users_block(i, configs["joiner4"]) for i in range(1, configs.get("filter-users", 0) + 1))
        for i in range(1, configs.get("filter-users", 0) + 1):
            sheeps_list.append(f"filter_users{i}")

        file.writelines(filter_stores_block(i, configs["joiner3"], configs["joiner4"]) for i in range(1, configs.get("filter-stores", 0) + 1))
        for i in range(1, configs.get("filter-stores", 0) + 1):
            sheeps_list.append(f"filter_stores{i}")

        file.writelines(filter_menu_items_block(i, configs["joiner2a"], configs["joiner2b"]) for i in range(1, configs.get("filter-menu-items", 0) + 1))
        for i in range(1, configs.get("filter-menu-items", 0) + 1):
            sheeps_list.append(f"filter_menu_items{i}")
        
        file.writelines(concat_block(i) for i in range(1, configs.get("concat1", 0) + 1))
        for i in range(1, configs.get("concat1", 0) + 1):
            sheeps_list.append(f"concat_{i}")
        
        file.writelines(sender_block(i, "1") for i in range(1, configs.get("sender1", 0) + 1))
        for i in range(1, configs.get("sender1", 0) + 1):
            sheeps_list.append(f"sender1_{i}")
        file.writelines(sender_block(i, "2a") for i in range(1, configs.get("sender2a", 0) + 1))
        for i in range(1, configs.get("sender2a", 0) + 1):
            sheeps_list.append(f"sender2a_{i}")
        file.writelines(sender_block(i, "2b") for i in range(1, configs.get("sender2b", 0) + 1))
        for i in range(1, configs.get("sender2b", 0) + 1):
            sheeps_list.append(f"sender2b_{i}")
        file.writelines(sender_block(i, "3") for i in range(1, configs.get("sender3", 0) + 1))
        for i in range(1, configs.get("sender3", 0) + 1):
            sheeps_list.append(f"sender3_{i}")
        file.writelines(sender_block(i, "4") for i in range(1, configs.get("sender4", 0) + 1))
        for i in range(1, configs.get("sender4", 0) + 1):
            sheeps_list.append(f"sender4_{i}")
        
        file.writelines(counter_block(i, "2a", configs["global-aggregator2a"]) for i in range(1, configs.get("counter2a", 0) + 1))
        for i in range(1, configs.get("counter2a", 0) + 1):
            sheeps_list.append(f"counter2a_{i}")
        file.writelines(counter_block(i, "2b", configs["global-aggregator2b"]) for i in range(1, configs.get("counter2b", 0) + 1))
        for i in range(1, configs.get("counter2b", 0) + 1):
            sheeps_list.append(f"counter2b_{i}")
        file.writelines(counter_block(i, "4", configs["global-aggregator4"]) for i in range(1, configs.get("counter4", 0) + 1))
        for i in range(1, configs.get("counter4", 0) + 1):
            sheeps_list.append(f"counter4_{i}")

        file.writelines(global_aggregator_block(i, "2a", configs["joiner2a"]) for i in range(1, configs.get("global-aggregator2a", 0) + 1))
        for i in range(1, configs.get("global-aggregator2a", 0) + 1):
            sheeps_list.append(f"global_aggregator2a_{i}")
        file.writelines(global_aggregator_block(i, "2b", configs["joiner2b"]) for i in range(1, configs.get("global-aggregator2b", 0) + 1))
        for i in range(1, configs.get("global-aggregator2b", 0) + 1):
            sheeps_list.append(f"global_aggregator2b_{i}")
        file.writelines(global_aggregator_block(i, "3", configs["joiner3"]) for i in range(1, configs.get("global-aggregator3", 0) + 1))
        for i in range(1, configs.get("global-aggregator3", 0) + 1):
            sheeps_list.append(f"global_aggregator3_{i}")
        file.writelines(global_aggregator_block(i, "4", configs["joiner4"]) for i in range(1, configs.get("global-aggregator4", 0) + 1))
        for i in range(1, configs.get("global-aggregator4", 0) + 1):
            sheeps_list.append(f"global_aggregator4_{i}")

        file.writelines(joiner_block(i, "2a") for i in range(1, configs.get("joiner2a", 0) + 1))
        for i in range(1, configs.get("joiner2a", 0) + 1):
            sheeps_list.append(f"joiner2a_{i}")
        file.writelines(joiner_block(i, "2b") for i in range(1, configs.get("joiner2b", 0) + 1))
        for i in range(1, configs.get("joiner2b", 0) + 1):
            sheeps_list.append(f"joiner2b_{i}")
        file.writelines(joiner_block(i, "3") for i in range(1, configs.get("joiner3", 0) + 1))
        for i in range(1, configs.get("joiner3", 0) + 1):
            sheeps_list.append(f"joiner3_{i}")
        file.writelines(joiner_block(i, "4") for i in range(1, configs.get("joiner4", 0) + 1))
        for i in range(1, configs.get("joiner4", 0) + 1):
            sheeps_list.append(f"joiner4_{i}")

        file.writelines(partial_aggregator_block(i, "3", configs["global-aggregator3"]) for i in range(1, configs.get("partial-aggregator3", 0) + 1))
        for i in range(1, configs.get("partial-aggregator3", 0) + 1):
            sheeps_list.append(f"partial_aggregator3_{i}")

        for i in range(1, configs.get("cliente", 0) + 1):
            client_line = client(i, external)

            file.writelines(client_line)

        file.write(networks(external))

    with open("sheeps.txt", "w") as sf:
        sf.write("\n".join(sheeps_list) + "\n")

    with open("members.txt", "w") as sf:
        sf.write("\n".join(members_list) + "\n")

if __name__ == "__main__":
    main()
