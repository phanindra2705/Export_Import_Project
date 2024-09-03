import pandas as pd
import snowflake.connector
from flask import jsonify, request
from __init__ import app

from app import (snowflake_user, snowflake_schema, snowflake_account, snowflake_database, snowflake_password,
                 snowflake_warehouse, app)


def create_users_table():
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username STRING PRIMARY KEY,
            password_hash STRING,
            gmail STRING
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


def create_import_table(username, table_name, df):
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        cursor = conn.cursor()

        # Create a new table in Snowflake
        columns = ", ".join([f"{col} STRING" for col in df.columns])

        # Replace invalid characters in table_name
        table_name = table_name.replace('-', '_')  # Replace dashes with underscores

        create_table_query = f"CREATE OR REPLACE TABLE {username}_{table_name} ({columns})"
        cursor.execute(create_table_query)

        # Insert data into the new table
        for i, row in df.iterrows():
            values = "', '".join(row.astype(str))
            insert_query = f"INSERT INTO {username}_{table_name} VALUES ('{values}')"
            cursor.execute(insert_query)

        # Close cursor and connection
        cursor.close()
        conn.close()

        return True
    except Exception as e:
        print(f"Error creating or inserting data into Snowflake table '{username}_{table_name}': {e}")
        return False


def get_snowflake_tables():
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[1] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables


def fetch_table_data(table_name):
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df





