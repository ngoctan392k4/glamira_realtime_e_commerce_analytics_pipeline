from src.kafka_to_kafka.yaml_config import load_config
from src.kafka_to_kafka.consumer_to_kafka import collect_message
import logging

#
# Initialize logging config
#
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    logging.info("Starting collecting message from source kafka")
    collect_message()