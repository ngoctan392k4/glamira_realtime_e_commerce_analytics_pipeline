import logging


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