import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, ArrayType
)

################################
# 1. CONFIGURATION
################################
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "local_product_view")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "tan123")

# PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_URL = f"jdbc:postgresql://host.docker.internal:5432/{PG_DB}"

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver"
}

CHECKPOINT_PATH = "hdfs://namenode:8020/user/spark/checkpoints/user_log_raw"

# logging.info(PG_USER)
# logging.info(PG_PASS)

################################
# 2. LOGGING
################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("KafkaRawDemo")

################################
# 3. SCHEMA
################################
option_schema = StructType([
    StructField("option_id", StringType()),
    StructField("option_label", StringType())
])

log_schema = StructType([
    StructField("id", StringType()),
    StructField("api_version", StringType()),
    StructField("collection", StringType()),
    StructField("current_url", StringType()),
    StructField("device_id", StringType()),
    StructField("email", StringType()),
    StructField("ip", StringType()),
    StructField("local_time", StringType()),
    StructField("option", ArrayType(option_schema)),
    StructField("product_id", StringType()),
    StructField("referrer_url", StringType()),
    StructField("store_id", StringType()),
    StructField("time_stamp", LongType()),
    StructField("user_agent", StringType())
])

################################
# 4. WRITE FUNCTION
################################
def write_raw_to_postgres(batch_df, batch_id):
    logging.info(f"PG_PASSWORD = {os.getenv('PG_PASSWORD', 'khong lay duoc')}")


    if batch_df.isEmpty():
        log.info(f"Batch {batch_id} empty → skip")
        return

    row_count = batch_df.count()
    log.info(f"Writing batch {batch_id}, rows={row_count}")

    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=PG_URL,
            table="user_log_raw",
            properties=PG_PROPS
        )
    )

################################
# 5. MAIN
################################
if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("KafkaToPostgresRawDemo")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    log.info("🚀 Starting Kafka → PostgreSQL RAW streaming")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")

        # 🔴 BẮT BUỘC nếu Kafka có SASL
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", os.getenv("KAFKA_SASL_JAAS_CONFIG"))

        .load()
    )


    # Parse JSON
    parsed_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), log_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "local_time",
            to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("option", to_json(col("option")))
    )

    # Write to Postgres
    parsed_stream.printSchema()
    
    query = (
        parsed_stream
        .writeStream
        .outputMode("append")
        .foreachBatch(write_raw_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()
