from src.kafka_to_kafka.yaml_config import load_config
from src.kafka_to_kafka.consumer_to_kafka import collect_message
import logging

#
# Initialize logging config
#
config = load_config()
log_cf = config.get("LOGGING", {})

logging.basicConfig(
    level=getattr(logging, log_cf.get("level","INFO").upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_cf["log_file"], encoding='utf-8'),
        logging.StreamHandler() if log_cf.get("to_console", False) else logging.NullHandler()
    ]
)

if __name__ == "__main__":
    logging.info("Starting collecting message from source kafka")
    collect_message()