

def read_config_file():
    print("Reading configuration file...")
    with open("compose.config", "r") as file:
        lines = file.readlines()
    return lines


def main():
    configs = read_config_file()
    print("Configuration values:")
    for line in configs:
        key, value = line.strip().split("=", 1)
        print(f"  {key}: {value}")

if __name__ == "__main__":
    main()