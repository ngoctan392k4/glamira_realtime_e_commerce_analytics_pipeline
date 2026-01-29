
import os
import logging 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType,  LongType


from streaming.spark_streaming import process_batch
from streaming.ip_loc_enricher import get_loc_info


#######################
# 1. CONFIGURATION    #
#######################
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASS = os.getenv("KAFKA_PASS")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
KAFKA_SASL_JAAS_CONFIG = os.getenv("KAFKA_SASL_JAAS_CONFIG")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASSWORD")

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
# PG_URL = f"jdbc:postgresql://host.docker.internal:5432/{PG_DB}"
PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver"
}

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL")
LOGGING_FILE = os.getenv("LOGGING_FILE")
LOGGING_TO_CONSOLE = os.getenv("LOGGING_TO_CONSOLE", "false").lower() == "true"

IP2LOC_DB_PATH = "IP-COUNTRY-REGION-CITY.BIN"
CHECKPOINT_PATH = "hdfs://namenode:8020/user/spark/checkpoints/user_log_raw"

logging.basicConfig(
    level=getattr(logging, LOGGING_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        # logging.FileHandler(LOGGING_FILE, encoding='utf-8'),
        logging.StreamHandler() if LOGGING_TO_CONSOLE else logging.NullHandler()
    ]
)

#######################
# 2. SCHEMA           #
#######################
location_schema = StructType([
    StructField("location_id", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_short", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True)
])

def get_log_schema():
    option_schema = StructType([
        StructField("option_id", StringType()),
        StructField("option_label", StringType())
    ])
    
    return StructType([
        StructField("id", StringType()),
        StructField("api_version", StringType()),
        StructField("collection", StringType()),
        StructField("current_url", StringType()),
        StructField("device_id", StringType()),
        StructField("user_id_db", StringType()),
        StructField("resolution", StringType()),      
        StructField("email_address", StringType()),
        StructField("ip", StringType()),
        StructField("local_time", StringType()),
        StructField("option", ArrayType(option_schema)),
        StructField("product_id", StringType()),
        StructField("referrer_url", StringType()),
        StructField("store_id", StringType()),
        StructField("time_stamp", LongType()),
        StructField("user_agent", StringType())
    ])
    
ip_loc_enricher = udf(
    lambda ip: get_loc_info(ip, IP2LOC_DB_PATH),
    location_schema
)  


def main():
    spark = SparkSession.builder \
        .appName("ETL_SparkStreamingWithKafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL) \
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM) \
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
        .load()

    parsed_stream = (
        raw_stream \
        .select(from_json(col("value").cast("string"), get_log_schema()).alias("data")) 
        .select("data.*")
        .withColumn("loc_info", ip_loc_enricher(col("ip")))
    )

    query = (
        parsed_stream.writeStream 
        .foreachBatch(process_batch) 
        .option("checkpointLocation", CHECKPOINT_PATH) 
        .trigger(processingTime="15 seconds") 
        .start()
    )

    query.awaitTermination()