import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time
import plotly.express as px
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
load_dotenv()

#######################
# 1. CONFIGURATION    #
#######################

st.set_page_config(
    page_title="Real-Time Glamira E-commerce Report",
    layout="wide",
)

# PG_HOST = os.getenv("PG_HOST")
PG_HOST = os.getenv("PG_HOST_TO_DOCKER")
PG_PORT = os.getenv("PG_PORT_TO_DOCKER")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASSWORD")


#######################
# 2. HELPER FUNCTION  #
#######################
@st.cache_resource
def get_database_engine():
    print("DEMO", type(PG_PASS), PG_PASS)
    encoded_pwd = quote_plus(PG_PASS) # Encode pwd to ensure to correct format in connection url: EX: pwd is @abc => @ in pwd can cause error in connection
    db_url = f"postgresql://{PG_USER}:{encoded_pwd}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(db_url)
    return engine


def run_query(query, params=None):
    engine = get_database_engine()
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"Error running query {query} since {e}")
        return pd.DataFrame()


# Sidebar with Filters
st.sidebar.title("Filters")
v_country = st.sidebar.text_input("Country (for no. store views):", value="United Kingdom")
v_product = st.sidebar.text_input("Product ID (for views per hour):", value="103508")

# Title
st.title("Real-Time Glamira E-commerce Report")
st.markdown("Data is updated every 5 seconds")

# Layout Design
# Row 1
col1, col2 = st.columns(2, gap="large")
with col1:
    top_products = st.empty()
with col2:
    top_countries = st.empty()

# Row 2
col3, col4 = st.columns(2, gap="large")
with col3:
    hourly_traffic = st.empty()
with col4:
    browser_status = st.empty()

# Row 3
col5, col6 = st.columns(2, gap="large")
with col5:
    top_referrers = st.empty()
with col6:
    stores_in_country = st.empty()

# Row 4
product_view_hourly = st.empty()


while True:
    current_time = time.time()

    # Top 10 Products
    sql_products = """
                   SELECT
                        f.product_id,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE AND f.product_id IS NOT NULL
                    GROUP BY f.product_id
                    ORDER BY views DESC
                    LIMIT 10;
                """
    df_products = run_query(sql_products)
    with top_products.container():
        st.subheader("Top 10 Products Today (Current date)")
        if not df_products.empty:
            st.bar_chart(df_products.set_index("product_id"))
        else:
            st.info("Waiting for collecting data")

    # Top Visited Countries
    sql_country = """
                    SELECT
                        l.country_name,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_location l ON f.location_id = l.location_id
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE
                    GROUP BY l.country_name
                    ORDER BY views DESC
                    LIMIT 10;
                """
    df_country = run_query(sql_country)
    with top_countries.container():
        st.subheader("Top 10 Visited Countries")
        if not df_country.empty:
            fig = px.pie(df_country, values='views', names='country_name', hole=0.4)
            st.plotly_chart(fig, use_container_width=True, key=f"country_pie")
        else:
            st.info("Waiting for collecting data")

    # Hourly Traffic
    sql_hourly_traffic = """
                    SELECT
                        EXTRACT(HOUR FROM f.time_stamp) AS view_hour,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE
                    GROUP BY view_hour
                    ORDER BY view_hour;
                 """
    df_hourly_traffic = run_query(sql_hourly_traffic)
    with hourly_traffic.container():
        st.divider()
        st.subheader("Traffic by Hour")
        if not df_hourly_traffic.empty:
            all_hours = pd.DataFrame({'view_hour': range(24)})
            df_hourly_traffic = pd.merge(all_hours, df_hourly_traffic, on='view_hour', how='left').fillna(0)
            st.bar_chart(df_hourly_traffic.set_index("view_hour"))
        else:
            st.info("Waiting for collecting data")

    # Browser status
    sql_browser = """
                    SELECT
                        c.user_agent,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_customer c ON f.customer_id = c.customer_id
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE
                    GROUP BY c.user_agent
                    ORDER BY views DESC;
                  """
    df_browser = run_query(sql_browser)
    with browser_status.container():
        st.divider()
        st.subheader("Types of Browser")
        if not df_browser.empty:
            st.dataframe(df_browser, use_container_width=True)
        else:
            st.info("Waiting for collecting data")

    # Top 5 Referrer URL
    sql_referrer = """
                    SELECT
                        f.referrer_url,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE
                    GROUP BY f.referrer_url
                    ORDER BY views DESC
                    LIMIT 5;
                   """
    df_ref = run_query(sql_referrer)
    with top_referrers.container():
        st.divider()
        st.subheader("Top 5 Referrer URLs")
        if not df_ref.empty:
            st.dataframe(df_ref, use_container_width=True, hide_index=True)
        else:
            st.info("Waiting for collecting data")

    # 6. No of views by stores and country
    sql_store = """
                    SELECT
                        f.store_id,
                        COUNT(*) AS views
                    FROM fact_product_views f
                    JOIN dim_location l ON f.location_id = l.location_id
                    JOIN dim_date d ON f.date_id = d.date_id
                    WHERE d.full_date = CURRENT_DATE AND l.country_name = %(country)s
                    GROUP BY f.store_id
                    ORDER BY views DESC;
                """
    df_store = run_query(sql_store, params={'country': v_country})
    with stores_in_country.container():
        st.divider()
        st.subheader(f"No of views by stores in {v_country}")
        if not df_store.empty:
            st.bar_chart(df_store.set_index("store_id"))
        else:
            st.warning(f"No data found for {v_country}")

    # Hourly View for Specific Product
    sql_prod_hourly = """
                        SELECT
                            EXTRACT(HOUR FROM f.time_stamp) AS view_hour,
                            COUNT(*) AS views
                        FROM fact_product_views f
                        JOIN dim_date d ON f.date_id = d.date_id
                        WHERE d.full_date = CURRENT_DATE AND f.product_id = %(pid)s
                        GROUP BY view_hour
                        ORDER BY view_hour;
                      """
    df_pid_hour = run_query(sql_prod_hourly, params={'pid': v_product})
    with product_view_hourly.container():
        st.divider()
        st.subheader(f"Hourly Views by Product ID {v_product}")
        if not df_pid_hour.empty:
            all_hours = pd.DataFrame({'view_hour': range(24)})
            df_pid_hour = pd.merge(all_hours, df_pid_hour, on='view_hour', how='left').fillna(0)

            st.bar_chart(df_pid_hour.set_index("view_hour"))
        else:
            st.warning(f"No data found for Product ID: {v_product}")

    time.sleep(5)