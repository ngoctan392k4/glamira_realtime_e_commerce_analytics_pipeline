import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

#############################
# HELPER
#############################

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

#############################
# YARN CHECK
#############################

def check_yarn():
    spark_config = Variable.get("spark", deserialize_json=True)
    yarn_rm = spark_config["yarn_rm_url"]

    try:
        response = requests.get(f"{yarn_rm}/ws/v1/cluster/info", timeout=5)
        response.raise_for_status()
        data = response.json()

        state = data["clusterInfo"]["state"]

        if state != "STARTED":
            send_telegram(f"YARN DOWN: state={state}")
            raise AirflowException("YARN not healthy")

        send_telegram(f"YARN is reachable: state={state}")

    except Exception as e:
        send_telegram(f"Cannot reach YARN: {str(e)}")
        raise AirflowException("YARN unreachable")

#############################
# HDFS (HADOOP) CHECK
#############################

def check_hdfs_namenode():
    spark_config = Variable.get("spark", deserialize_json=True)
    namenode_url = spark_config["hdfs_namenode_url"]
    try:
        response = requests.get(
            f"{namenode_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus",
            timeout=5
        )
        response.raise_for_status()
        data = response.json()

        state = data["beans"][0]["State"]

        if state != "active":
            send_telegram(f"HDFS NameNode NOT ACTIVE: {state}")
            raise AirflowException("HDFS NameNode not active")

        send_telegram(f"HDFS NameNode OK: {state}")

    except Exception as e:
        send_telegram(f"Cannot reach HDFS NameNode: {str(e)}")
        raise AirflowException("HDFS unreachable")


#############################
# AIRFLOW DAG
#############################

with DAG(
    dag_id="spark_hadoop_yarn_health_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
) as dag:

    t_yarn = PythonOperator(
        task_id="check_yarn",
        python_callable=check_yarn,
        execution_timeout=timedelta(seconds=20),
    )

    t_hdfs = PythonOperator(
        task_id="check_hdfs_namenode",
        python_callable=check_hdfs_namenode,
        execution_timeout=timedelta(seconds=20),
    )

    # Pipeline
    t_yarn >> t_hdfs


