from datetime import datetime, timedelta
import os
import logging 
import psycopg2
import hashlib
import IP2Location

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, udf, lit, to_timestamp, hour, year, month, dayofmonth, date_format
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType,  IntegerType, LongType, FloatType


#######################
# 1. CONFIGURATION    #
#######################
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_SUBSCRIBE")
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

# PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_URL = f"jdbc:postgresql://host.docker.internal:5432/{PG_DB}"
PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver"
}

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL")
LOGGING_FILE = os.getenv("LOGGING_FILE")
LOGGING_TO_CONSOLE = os.getenv("LOGGING_TO_CONSOLE", "false").lower() == "true"

IP2LOC_DB_PATH = "/spark/resources/data/IP2LOCATION-LITE-DB11.BIN"
CHECKPOINT_PATH = "hdfs://namenode:8020/user/spark/checkpoints/user_log_raw"



def get_loc_info(ip_address, db_path):
    try:
        if not os.path.exists(db_path):
            logging.warning("IP2LOC_DB DOES NOT EXIST")
            return None

        ipdb = IP2Location.IP2Location(db_path)
        info = ipdb.get_all(ip_address)

        if info:
            country = info.country_long if info.country_long else ""
            region = info.region if info.region else ""
            city = info.city if info.city else ""
            
            loc_string = f"{country}|{region}|{city}"
            loc_id = hashlib.md5(loc_string.encode('utf-8')).hexdigest()

            return (
                loc_id, info.country_short, info.country_long, info.region, info.city
            )
    except Exception as e:
        logging.error(ip_address, "IS NOT FOUND")
        return None
    return None


def store_transformer(store_id):
    store_name = "Store " + store_id
    return store_name


def customer_transformer(customer_id, email_address, user_agent, user_id_db, resolution):
    return {
        'customer_id': customer_id if customer_id else '-1',
        'email_address': email_address if email_address and email_address.strip() else 'Not Defined',
        'user_agent': user_agent if user_agent and user_agent.strip() else 'Not Defined',
        'user_id_db': user_id_db if user_id_db and user_id_db.strip() else 'Not Defined',
        'resolution': resolution if resolution and resolution.strip() else 'Not Defined'
    }


def date_transformer(time_stamp): 
    if not time_stamp:
        return None
    
    if isinstance(time_stamp, str):
        time_stamp = datetime.fromisoformat(time_stamp)
    
    day_of_week_num = time_stamp.weekday()
    is_weekend = day_of_week_num >= 5
    day_of_year = time_stamp.timetuple().tm_yday
    week_of_year = time_stamp.isocalendar()[1]
    quarter_number = (time_stamp.month - 1) // 3 + 1
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_abbr = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    return {
        'date_id': time_stamp.strftime('%Y%m%d'),
        'full_date': time_stamp.date(),
        'date_of_week': day_names[day_of_week_num],
        'date_of_week_short': day_names_abbr[day_of_week_num],
        'is_weekday_or_weekend': 'weekend' if is_weekend else 'weekday',
        'day_of_month': time_stamp.day,
        'day_of_year': day_of_year,
        'week_of_year': week_of_year,
        'quarter_number': quarter_number,
        'year_number': time_stamp.year,
        'year_month': time_stamp.strftime('%Y%m')
    }
    

def upsert_location_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
                INSERT INTO dim_location (location_id, country_name, country_short, region_name, city_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (location_id) DO NOTHING;
                """
        cur.execute(sql, values)
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()

def upsert_product_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
                INSERT INTO dim_product (product_id) VALUES (%s) ON CONFLICT (product_id) DO NOTHING
                VALUES (%s) ON CONFLICT (product_id) DO NOTHING;
                """
        cur.execute(sql, values)
        
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        
def upsert_store_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
                INSERT INTO dim_store (store_id, store_name)
                VALUES (%s, %s) ON CONFLICT (store_id) DO NOTHING;
                """
        cur.execute(sql, values)

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        
        
def upsert_date_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
                INSERT INTO dim_date (date_id, full_date, date_of_week, date_of_week_short, is_weekday_or_weekend, day_of_month, day_of_year, week_of_year, quarter_number, year_number, year_month)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (date_id) DO NOTHING;
                """
        cur.execute(sql, values)

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        
def upsert_customer_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
                INSERT INTO dim_customer (customer_id VARCHAR(255), email_address, user_agent, user_id_db, resolution, utm_source, utm_medium)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING;
                """
        cur.execute(sql, values)

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()



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

log_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("current_url", StringType(), True)
])

location_schema = StructType([
    StructField("loc_id", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_short", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True)
])

def get_schema():
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
    
ip_loc_enricher = udf(
    lambda ip: get_loc_info(ip, IP2LOC_DB_PATH),
    location_schema
)  


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ETL_SparkStreamingWithKafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL) \
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM) \
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
        .load()

    parsed_stream = (
        raw_stream \
        .select(from_json(col("value").cast("string"), log_schema).alias("data")) 
        .select("data.*")
        .withColumn("location_data", ip_loc_enricher(col("ip")))
    )

    query = (
        parsed_stream.writeStream 
        .foreachBatch(process_batch) 
        .option("checkpointLocation", CHECKPOINT_PATH) 
        .trigger(processingTime="15 seconds") 
        .start()
    )

    query.awaitTermination()