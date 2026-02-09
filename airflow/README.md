- [Orchestration with Airflow](#orchestration-with-airflow)
- [Table of Contents](#table-of-contents)
  - [❓ Problem Description](#-problem-description)
  - [⚙️ Preparation for project](#️-preparation-for-project)
    - [Step 1: Install Hadoop](#step-1-install-hadoop)
    - [Step 2: Install Spark](#step-2-install-spark)
    - [Step 3: Install Postgresql](#step-3-install-postgresql)
    - [Step 4: Initialize .env file from the root folder](#step-4-initialize-env-file-from-the-root-folder)
    - [Step 5: Install Airflow](#step-5-install-airflow)
    - [Step 6: Initialize Variables on Airflow](#step-6-initialize-variables-on-airflow)
  - [📝 How to run](#-how-to-run)
    - [Step 1: Start docker compose airflow](#step-1-start-docker-compose-airflow)
    - [Step 2: Start docker compose Hadoop](#step-2-start-docker-compose-hadoop)
    - [Step 3: Access to Airflow UI and start DAGs](#step-3-access-to-airflow-ui-and-start-dags)
  - [References](#references)
 
# Orchestration with Airflow 
# Table of Contents
## ❓ Problem Description
In a real-time data pipeline using Kafka, Spark, and Airflow, failures in any component can lead to data loss, delays, or system downtime. The lack of automated monitoring and alerting in data pipeline make it difficult to detect issues in time.

Orchestration with Airflow project implements automated health checks for Kafka and Spark, monitor data flow, and set up Telegram alerts to ensure a reliable and observable data pipeline.

## ⚙️ Preparation for project
### Step 1: Install Hadoop
- Stand at [Hadoop Folder](./build/hadoop/)
- See the instruction in [Hadoop Installing](./build/hadoop/README.md)

### Step 2: Install Spark
- Stand at [Spark Folder](./build/spark)
- See the instruction in [Spark Installing](./build/spark/README.md)

### Step 3: Install Postgresql
- Stand at [Postgresql Folder](./build/kafka)
- See the instruction in [Postgresql Installing](./build/kafka/README.md)

### Step 4: Initialize .env file from the root folder
Example of .env file
```
AIRFLOW_IMAGE_NAME=airflow:2.10.4
AIRFLOW_UID=1000
DOCKER_GID=999
```

### Step 5: Install Airflow
- Stand at the root folder of the project
- Create network with the following bash:
  ```shell
  docker network create streaming-network --driver bridge
  ```

- Build custom docker image 
  ```
  docker build -t airflow:2.10.4 .
  ```

- Initializing Environment
  - Setting the right Airflow user
    - Making folders `dags`, `logs`, `plugins`, `config`
      ```shell
      mkdir -p ./dags ./logs ./plugins ./config
      ```

    - Fetching user id information and group id of docker group with the following bash:
      ```shell
      id -u
      ```
      ```shell
      getent group docker
      ```

    - Set output into variables `AIRFLOW_UID` and `DOCKER_GID` in file `.env`

  - Initialize airflow.cfg
    ```shell
    docker compose run airflow-cli bash -c "airflow config list > /opt/airflow/config/airflow.cfg"
    ```

- Initialize the database
  ```shell
  docker compose up airflow-init
  ```

- Running Airflow
  ```shell
  docker compose up -d
  ```

  **Running docker containers:**
  ```
  airflow-scheduler - The scheduler monitors all tasks and dags, then triggers the task instances once their dependencies
  are complete.

  airflow-dag-processor - The DAG processor parses DAG files.

  airflow-api-server - The api server is available at http://localhost:18080.

  airflow-worker - The worker that executes the tasks given by the scheduler.

  airflow-triggerer - The triggerer runs an event loop for deferrable tasks.

  airflow-init - The initialization service.

  postgres - The database.

  redis - The redis - broker that forwards messages from scheduler to worker.
  ```
  
  **Check Status**
  ```shell
  docker compose ps
  ```
**IMPORTANCE** 
- Install Airflow on Linux operating system
- Install Airflow on the Linux operating system partition if using different operating systems on the same computer. Airflow cannot run on NTFS formatted disks like Windows.

### Step 6: Initialize Variables on Airflow
- Access Web interface of Airflow: [web interface](http://localhost:18080) with username/password: `airflow/airflow`
- Go to Admin => Variables => Add new
- Example of key ```kafka```
  ```
    key: "kafka"
    Val:
    {
      "host": "localhost",
      "ports": [9094, 9194, 9294],
      "topic": "product_view",
      "group_id": "ktok_v12",
      "security_protocol": "SASL_PLAINTEXT",
      "sasl_mechanism": "PLAIN",
      "sasl_plain_username": "kafka",
      "sasl_plain_password": "Kafka2026"
    }
  ```

- Example of key ```telegram```
  ```
    key: "telegram"
    Val:
    {
      "token": "your_bot_token",
      "chat_id": your chat id with bot
    }
  ```

- Example of key ```Spark```
  ```
    key: "spark"
    Val:
    {
      "yarn_rm_url": "http://resourcemanager:8088",
      "hdfs_namenode_url": "http://namenode:9870"   
    }
  ```

## 📝 How to run
### Step 1: Start docker compose airflow
- Stand at the root folder of the project and run the following bash
  ```bash
  docker compose start
  ```

### Step 2: Start docker compose Hadoop
- Move to hadoop build folder by running the following bash
  ```bash
  cd build/hadoop
  docker compose start
  ```
### Step 3: Access to Airflow UI and start DAGs
- Accessing the web interface: [web interface](http://localhost:18080) with username/password: `airflow/airflow`
- Select DAG named ```health_check_pipeline``` and click on the toggle to start the job
- Monitor the job and check logs at [Airflow_logs](./logs/)

## References
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Resource Manager Status Check](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html)
- [Namenode Status Check](https://stackoverflow.com/questions/37278690/how-to-check-the-namenode-status)



