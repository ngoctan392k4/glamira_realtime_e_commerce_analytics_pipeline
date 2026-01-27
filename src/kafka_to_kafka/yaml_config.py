import yaml

def load_config(path="environment/config.yml"):
    with open(path, "r", encoding='utf-8') as rf:
        return yaml.safe_load(rf)