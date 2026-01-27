from kafka import KafkaConsumer, KafkaProducer
from src.kafka_to_kafka.yaml_config import load_config
import logging
import json
import time

config = load_config()
KAFKA_TOPIC = config["KAFKA_TOPIC"]
LOCAL_KAFKA_TOPIC = config["LOCAL_KAFKA_TOPIC"]
SOURCE_BOOTSTRAP_SERVERS = config["SOURCE_BOOTSTRAP_SERVERS"]
LOCAL_BOOTSTRAP_SERVERS = config["LOCAL_BOOTSTRAP_SERVERS"]
SECURITY_PROTOCOL = config["SECURITY_PROTOCOL"]
SASL_MECHANISM = config["SASL_MECHANISM"]
SASL_PLAIN_USERNAME = config["SASL_PLAIN_USERNAME"]
SASL_PLAIN_PASSWORD = config["SASL_PLAIN_PASSWORD"]
SASL_PLAIN_USERNAME_LOCAL = config["SASL_PLAIN_USERNAME_LOCAL"]
SASL_PLAIN_PASSWORD_LOCAL = config["SASL_PLAIN_PASSWORD_LOCAL"]
BATCH_SIZE = 500

#
# SASL configuration
#
def get_sasl_config_local():
    return {
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": SASL_PLAIN_USERNAME_LOCAL,
        "sasl_plain_password": SASL_PLAIN_PASSWORD_LOCAL
    }
    
def get_sasl_config():
    return {
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": SASL_PLAIN_USERNAME,
        "sasl_plain_password": SASL_PLAIN_PASSWORD
    }
def collect_message():
    # Initialize consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers = SOURCE_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id='ktok_v12',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            **get_sasl_config()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")

    # Initialize producer
    try:
        producer = KafkaProducer(
            bootstrap_servers= LOCAL_BOOTSTRAP_SERVERS,
            value_serializer=lambda m: json.dumps(m).encode('ascii'),
            **get_sasl_config_local()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")


    # Consuming message and produce to local kafka
    batch_counter = 0
    try:
        while True:
            messages = consumer.poll(timeout_ms=5000)

            if not messages:
                logging.warning("No new messages")
                continue

            for topic_partition, messages in messages.items():
                for message in messages:
                    try:
                        producer.send(LOCAL_KAFKA_TOPIC, value=message.value)
                        batch_counter += 1
                    except Exception as e:
                        logging.exception(f"Error sending message to producer buffer")

            if batch_counter >= BATCH_SIZE or (not messages and batch_counter > 0):
                try:
                    producer.flush()
                    consumer.commit()

                    logging.info(f"Successfully produced and committed {batch_counter} data")
                    batch_counter = 0

                except Exception as e:
                    logging.exception(f"Error processing batch. Retrying in next loop...")
                    time.sleep(5)


    except Exception as e:
        logging.info(f"Error: {e}")
    finally:
        if batch_counter > 0:
            try:
                producer.flush()
                consumer.commit()

                logging.info(f"Successfully produced and committed {batch_counter} data")
            except Exception as e:
                logging.exception(f"Error when sending message to buffer")

        consumer.close()
        producer.close()
        logging.info(f"Have done")


    # Consuming message and produce to local kafka
    # try:
    #     for message in consumer:
    #         producer.send('product_view', message.value)
    #         producer.flush()
    #         count+=1
    #         logging.info(f"Have saved {count}")
    # except Exception as e:
    #     logging.exception(f"Interruption")
    # finally:
    #     consumer.close()
    #     producer.close()
    #     logging.info(f"Have done")