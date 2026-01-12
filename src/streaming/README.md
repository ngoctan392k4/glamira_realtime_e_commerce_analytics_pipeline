# Table of Contents
- [Table of Contents](#table-of-contents)
  - [1. Check connection to the Kafka server.](#1-check-connection-to-the-kafka-server)
  - [2. Run streaming program](#2-run-streaming-program)
  - [Run temp code to check the flow](#run-temp-code-to-check-the-flow)

## 1. Check connection to the Kafka server.

Check connection to 3 brokers

```shell
telnet HOST1 PORT1
telnet HOST2 PORT2
telnet HOST3 PORT3 
```

## 2. Run streaming program

```shell
docker container stop kafka-streaming || true &&
docker container rm kafka-streaming || true &&
docker run -ti --name kafka-streaming \
--env-file ./.env \
--network=streaming-network \
-v ./:/spark \
-v ./hadoop-conf:/spark/hadoop-conf \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS='kafka-0:9092,kafka-1:9192,kafka-2:9292' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="@2025";' \
unigap/spark:3.5 bash -c "(cd /spark/src zip -r /tmp/streaming.zip streaming) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/streaming.zip \
--deploy-mode client \
--master yarn \
/spark/src/__main__.py"
```

## Run temp code to check the flow
```shell
docker container stop kafka-streaming || true &&
docker container rm kafka-streaming || true &&
docker run -ti --name kafka-streaming \
--env-file ./.env \
--network=streaming-network \
-v ./:/spark \
-v ./hadoop-conf:/spark/hadoop-conf \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS='kafka-0:9092,kafka-1:9192,kafka-2:9292' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="@2025";' \
unigap/spark:3.5 bash -c "
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--deploy-mode client \
--master yarn \
/spark/src/temp/spark_raw_log.py"

```