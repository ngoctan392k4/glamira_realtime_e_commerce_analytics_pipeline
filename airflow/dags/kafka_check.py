import socket
import requests
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer
from airflow.exceptions import AirflowException

#####################
# HELPER FUNCTION
#####################

def send_telegram(message):
    telegram_config = Variable.get("telegram", deserialize_json=True)
    token = telegram_config["token"]
    chat_id = telegram_config["chat_id"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    requests.post(
        url,
        json={"chat_id": chat_id, "text": message},
        timeout=5
    )


#################################
# Kafka Health Check FUNCTION
#################################

# Check connection to kafka with host and ports
def check_host_ports(timeout=5):
    connected = False
    kafka_config = Variable.get("kafka", deserialize_json=True)
    ports = kafka_config["ports"]
    host = kafka_config["host"]
    
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            sock.connect((host, port))
            sock.close()
            connected = True
            message = f"SUCCESSFULLY CONNECTION to Kafka {host}:{port}\n"
            send_telegram(message)
            
        except Exception as e:
            message = f"FAILED CONNECTION to Kafka {host}:{port}\n"
            send_telegram(message)
            sock.close()

    if connected:
        message = f"Kafka {host}:{ports} can be connected\n"
        send_telegram(message)        

# Check topics and brokers
def kafka_broker_topic_check():
    kafka_config = Variable.get("kafka", deserialize_json=True)
    ports = kafka_config["ports"]
    host = kafka_config["host"]
    topic = kafka_config["topic"]
    
    bootstrap_servers = [f"{host}:{port}" for port in ports]

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="kafka",
            sasl_plain_password="UnigapKafka@2024",
            request_timeout_ms=5000
        )

        send_telegram("Connected to Kafka Admin")
    except Exception as e:
        send_telegram(f"KafkaAdminClient FAILED: {str(e)}")
        raise AirflowException("Kafka Admin connection failed")


    cluster = admin.describe_cluster()

    if not cluster["brokers"]:
        send_telegram("No Kafka broker available")
        raise AirflowException("No Kafka broker available")

    
    existing_topics = admin.list_topics()
    if topic not in existing_topics:
        send_telegram(f"Topic {topic} does not exist")
        raise AirflowException(f"Topic {topic} does not exist")

    send_telegram(f"Topic {topic} exist")

# Check consumer lag
def kafka_consumer_lag_check():
    kafka_config = Variable.get("kafka", deserialize_json=True)
    ports = kafka_config["ports"]
    host = kafka_config["host"]
    topic = kafka_config["topic"]
    group_id = kafka_config["group_id"]
    
    bootstrap_servers = [f"{host}:{port}" for port in ports]
    
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="kafka",
        sasl_plain_password="UnigapKafka@2024",
        enable_auto_commit=False
    )

    consumer.poll(timeout_ms=1000)
    consumer.assign(consumer.assignment())

    total_lag = 0
    for tp in consumer.assignment():
        end = consumer.end_offsets([tp])[tp]
        committed = consumer.committed(tp) or 0
        total_lag += end - committed

    send_telegram(f"Consumer lag: {total_lag}")

#################################
# Airflow DAG
#################################
with DAG(
    dag_id="kafka_health_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *", 
    catchup=False,
) as dag:
    connection_check = PythonOperator(
        task_id="check_kafka_connection",
        python_callable=check_host_ports,
    )
    
    existence_check = PythonOperator(
        task_id="check_broker_topic_existence",
        python_callable=kafka_broker_topic_check,
        execution_timeout=timedelta(seconds=20),
    )
    
    consumer_check = PythonOperator(
        task_id="consumer_lag_check",
        python_callable=kafka_consumer_lag_check,
        execution_timeout=timedelta(seconds=20),
    )
    
    
    connection_check >> existence_check >> consumer_check
