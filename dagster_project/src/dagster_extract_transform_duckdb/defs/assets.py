import pandas as pd
import dagster as dg
import numpy as num
import duckdb as db
import shopify as shop
import requests as req
import os
from datetime import datetime
from dotenv import load_dotenv

@dg.asset
def establish_duck_db_conn():
    ## Adjust current working directory to point to Database file folder location.
    os.chdir('..')
    
    subfolder_name = "duckdb_database"

    os.chdir(subfolder_name)

    ## Create a connection to a persisted DuckDB database, setup for local execution and storage.
    con = db.connect("database.db")
    
    ## Pull test data related to table metadata in database to ensure proper connection.
    tables_info_df = con.execute("SELECT * FROM duckdb_tables;").fetchdf()
    tables_info_df = pd.DataFrame(tables_info_df)
    
    ## Test validity of non-NULL database connection.
    def test_dataframe_not_empty(df):
        return not df.empty

    test_df_availability_status = test_dataframe_not_empty(tables_info_df)

    if test_df_availability_status != True:
        raise dg.Failure(
            f"Database Connection issue: Null Data"
        )

    ## Close the current DuckDB connection
    con.close()

    return "Database connection successful"


@dg.asset()
def establish_shopify_api_conn():
    ## Load variable from .env file
    load_dotenv()

    ## Specify Shopify store API metadata for conn; for testing purposes a dummy dev store has been setup with the following properties.
    shop_url = os.getenv("SHOPIFY_API_URL")
    api_version = os.getenv("SHOPIFY_API_VERSION")
    api_key = os.getenv("SHOPIFY_API_KEY")
    # Create a Shopify session
    session = shop.Session(shop_url, api_version, api_key)

    # Activate the session
    shop.ShopifyResource.activate_session(session)

    # Interact with the Shopify API to extract the store name and verify successful connection.
    shop_id = shop.Shop.current()
    shop_name = shop_id.name

    ## Note that the below business name is just a testing instantiation and would require updating for production.
    if shop_name != 'No Name Business 123':
        raise dg.Failure(
            f"Shopify API connection failure"
        )

    return "Shopify API connection success"


@dg.asset(deps=[establish_duck_db_conn, establish_shopify_api_conn])
def extract_load_shopify_orders_data():
    ## Load variable from .env file
    load_dotenv()

    ## Re-establish Shopify API connection for Products data extraction. Utilize connection via Python requests package for ease of API json extraction.
    shop_url = os.getenv("SHOPIFY_API_URL")
    api_version = os.getenv("SHOPIFY_API_VERSION")
    api_key = os.getenv("SHOPIFY_API_KEY")

    headers = {
        "X-Shopify-Access-Token": api_key,
        "Content-Type": "application/json"
    }

    ## Fetching orders data by SKU line items
    orders_url = f"https://{shop_url}/admin/api/{api_version}/orders.json"
    response = req.get(orders_url, headers=headers)
    orders_data = response.json().get("orders", [])
    orders_df = pd.DataFrame(orders_data)
    line_item_orders_df = pd.json_normalize(orders_df.line_items[0])

    ## Add a snapshot data for data versioning control downstream
    line_item_orders_df['snapshot_date'] = datetime.now()

    ## Re-establish DuckDB database connection for data loading and downstream transformation.
    os.chdir('..')
    subfolder_name = "duckdb_database"
    os.chdir(subfolder_name)

    con = db.connect("database.db")

    ## Ensure the creation of an entities schema in the persisted DuckDB database to store data as a 1:1 relationship with upstream source.
    entities_schema_name = "entities"
    con.execute("CREATE SCHEMA IF NOT EXISTS entities_schema_name;")

    ## Ensure the creation of an entities data table for Shopify Products in the persisted DuckDB database with a 1:1 relationship with the upstream Shopify Orders API. 
    con.execute("CREATE TABLE IF NOT EXISTS entities.shopify_order_line_items AS SELECT * FROM line_item_orders_df;")

    try:
        con.execute("ALTER TABLE entities.shopify_order_line_items ADD PRIMARY KEY(id, snapshot_date);")
    except db.duckdb.CatalogException:
        pass

    con.execute("INSERT OR REPLACE INTO entities.shopify_order_line_items SELECT * FROM line_item_orders_df;")

    ## Add a snapshot data for data versioning control downstream
    orders_df['snapshot_date'] = datetime.now()

    ## Re-establish DuckDB database connection for data loading and downstream transformation.
    os.chdir('..')
    subfolder_name = "duckdb_database"
    os.chdir(subfolder_name)

    con = db.connect("database.db")

    ## Ensure the creation of an entities schema in the persisted DuckDB database to store data as a 1:1 relationship with upstream source.
    entities_schema_name = "entities"
    con.execute("CREATE SCHEMA IF NOT EXISTS entities_schema_name;")

    ## Ensure the creation of an entities data table for Shopify Products in the persisted DuckDB database with a 1:1 relationship with the upstream Shopify Orders API. 
    con.execute("CREATE TABLE IF NOT EXISTS entities.shopify_orders AS SELECT * FROM orders_df;")

    try:
        con.execute("ALTER TABLE entities.shopify_orders ADD PRIMARY KEY(id, snapshot_date);")
    except db.duckdb.CatalogException:
        pass

    con.execute("INSERT OR REPLACE INTO entities.shopify_orders SELECT * FROM orders_df;")

    ## Close the current DuckDB connection
    con.close()

    return "Shopify - Orders data successfully written to DuckDB database"


@dg.asset(deps=[extract_load_shopify_orders_data])
def extract_for_parabola_ingestion():
    ## Re-establish DuckDB database connection for data loading and downstream transformation.
    os.chdir('..')
    subfolder_name = "duckdb_database"
    os.chdir(subfolder_name)

    con = db.connect("database.db")

    ## Create a new dataframe to store Shopify Order Line Items data
    order_line_items_df = con.sql("SELECT * FROM entities.shopify_order_line_items;").df()

    ## Change working directory to ensure CSV write is located in the repos root directory.
    os.chdir('..')
    subfolder_name = "extracted_data_for_parabola"
    os.chdir(subfolder_name)

    ## Write the dataframe to a persisted CSV file. Note that the location of this CSV is immutable as it is essential for Parabola worflow ingestion success.
    order_line_items_df.to_csv('shopify_order_line_items_raw_data.csv', index=False)

    return "Shopify - Orders data successfully extract from DuckDB database and written to CSV"
