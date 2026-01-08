import os
import logging 
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


from streaming.spark_upserter import upsert_date_dimension, upsert_location_dimension, upsert_product_dimension, upsert_store_dimension, upsert_customer_dimension
from streaming.spark_transformer import store_transformer, customer_transformer, date_transformer

#######################
# 1. CONFIGURATION    #
#######################
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_SUBSCRIBE", "product_view")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASS = os.getenv("KAFKA_PASS")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "password")

# PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_URL = f"jdbc:postgresql://host.docker.internal:5432/{PG_DB}"
PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver"
}    

#######################
# 2. HELPER FUNCTION  #
#######################  

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logging.info(f"{batch_id} iS EMPTY")
        return

    logging.info(f"PROCESSING {batch_id}")
    batch_df.cache()

    # Current url for getting domain
    # Collection for getting log types
    rows = batch_df.select(
        "collection", "current_url", "referrer_url", "product_id", "store_id", "user_agent", "user_id_db", "resolution", "time_stamp", "email_address", "device_id",
        "loc_info.location_id", "loc_info.country_name", "loc_info.country_short", "loc_info.region_name",
        "loc_info.city_name"
    ).collect()
    

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )

    # Maps
    product_map = {}
    store_map = {}
    location_map = {}
    customer_map = {}
    date_map = {}


    for row in rows:
        # Transform and upsert PRODUCT
        if row.product_id and row.product_id not in product_map:
            logging.info("UPSERTING PRODUCT DIMENSION")
            pk = upsert_product_dimension(conn, "dim_product", "product_id", "product_id", row.product_id)
            product_map[row.product_id] = pk

        # Transform and upsert STORE
        if row.store_id and row.store_id not in store_map:
            store_name = store_transformer(row.store_id)
            store_tuple = (row.store_id, store_name)
            logging.info("UPSERTING STORE DIMENSION")
            sk = upsert_store_dimension(conn, "dim_store", "store_id", "store_id", None, store_tuple)
            store_map[row.store_id] = sk

        # Transform and upsert LOCATION
        if row.location_id and row.location_id not in location_map:
            loc_tuple = (row.location_id, row.country_name, row.country_short, row.region_name, row.city_name)
            logging.info("UPSERTING LOCATION DIMENSION")
            lk = upsert_location_dimension(conn, "dim_location", "location_id", "location_id", None, loc_tuple)
            location_map[row.location_id] = lk

        # Transform and upsert CUSTOMER
        if row.user_agent or row.device_id or row.email_address or row.user_id_db or row.resolution:
            customer_data = customer_transformer(
                customer_id = row.device_id,
                email_address = row.email_address,
                user_agent = row.user_agent,
                user_id_db = row.user_id_db,
                resolution = row.resolution
            )

            if customer_data.customer_id not in customer_map:
                customer_tuple = (
                    customer_data['customer_id'],
                    customer_data['email_address'],
                    customer_data['user_agent'],
                    customer_data['user_id_db'],
                    customer_data['resolution'],
                )
                logging.info("UPSERTING CUSTOMER DIMENSION")
                ck = upsert_customer_dimension(conn, "dim_customer", "customer_id", "customer_id", None, customer_tuple)
                customer_map[customer_data['customer_id']] = ck

        # Transform and upsert DATE
        if row.time_stamp:
            date_data = date_transformer(row.time_stamp)
            
            if date_data:
                date_id = date_data['date_id']
                
                if date_id not in date_map:
                    date_tuple = (
                        date_id,
                        date_data['full_date'],
                        date_data['date_of_week'],
                        date_data['date_of_week_short'],
                        date_data['is_weekday_or_weekend'],
                        date_data['day_of_month'],
                        date_data['day_of_year'],
                        date_data['week_of_year'],
                        date_data['quarter_number'],
                        date_data['year_number'],
                        date_data['year_month']
                    )
                    logging.info("UPSERTING DATE DIMENSION")
                    upsert_date_dimension(conn, "dim_date", "date_id", None, None, date_tuple)
                    date_map[date_id] = date_id

    conn.close()

    # Create DataFrames from maps
    spark = SparkSession.builder.getOrCreate()
    prod_df = spark.createDataFrame([(k, v) for k, v in product_map.items()], ["product_id", "product_key"])
    store_df = spark.createDataFrame([(k, v) for k, v in store_map.items()], ["store_id", "store_key"])
    loc_df = spark.createDataFrame([(k, v) for k, v in location_map.items()], ["loc_id", "loc_key"])
    cus_df = spark.createDataFrame([(k, v) for k, v in customer_map.items()], ["cus_id", "cus_key"])
    date_df = spark.createDataFrame([(k, v) for k, v in date_map.items()], ["date_id", "date_key"])

    # Join & Transform
    enriched_df = batch_df \
        .join(prod_df, batch_df.product_id == prod_df.product_id, "left") \
        .join(store_df, batch_df.store_id == store_df.store_id, "left") \
        .join(loc_df, batch_df.loc_info.location_id == loc_df.loc_id, "left") \
        .join(cus_df, batch_df.device_id == cus_df.cus_id, "left") \
        .join(date_df, batch_df.time_stamp.cast("date") == date_df.date_id, "left") \
        .select(
            col("product_id"),
            col("store_id"),
            col("loc_id"),
            col("cus_id"),
            col("date_id"),
            col("ip").alias("ip_address"),
            col("time_stamp")
        )

    logging.info("UPSERTING FACT PRODUCT VIEW DATA")
    try:
        enriched_df.write \
            .mode("append") \
            .jdbc(PG_URL, "fact_product_views", properties=PG_PROPS)
    except Exception as e:
        logging.exception(f"WRITING ERROR WITH BATCH {batch_id}: {e}")

    batch_df.unpersist()
