import psycopg2
import pandas as pd
import numpy as np
from configparser import ConfigParser


def load_config(filename='database_v2.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config


def connect(config):
    try:
        conn = psycopg2.connect(**config)
        # Now you can use the connection (e.g., create a cursor and execute queries)
        return conn
    except Exception as e:
        print(f"Error: {e}")


def insert_record(cursor, table_name, columns, values):
    placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for each column
    command = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.execute(command, tuple(values))


def insert_records_from_df(cursor, df: pd.DataFrame, table_name):
    columns = df.columns.values
    df = df.replace(np.nan, None)
    for _, row in df.iterrows():
        insert_record_with_company_id(cursor, table_name, columns, row)


def insert_record_with_company_id(cursor, table_name, columns, values):
    placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for each column

    if "company_id" in columns:
        raise ValueError("company_id exists in columns, this is a foreign key")
    if "symbol" not in columns:
        raise ValueError("symbol not found in columns")
    symbol_value = [values.values[ind] for ind, column in enumerate(columns) if column == "symbol"]
    symbol_value = symbol_value[0]
    command = (f"INSERT INTO {table_name} ({'company_id, ' + ', '.join(columns)}) "
               f"VALUES ((SELECT c.id FROM company c WHERE c.symbol = '{symbol_value}'), {placeholders})")
    try:
        cursor.execute(command, tuple(values))
    except psycopg2.errors.NotNullViolation:
        cursor.execute(f"INSERT INTO company (symbol) VALUES ('{symbol_value}')")
        cursor.execute(command)


def insert_records_from_df_given_symbol(cursor, df: pd.DataFrame, table_name, symbol):
    columns = df.columns.values
    df = df.replace(np.nan, None)
    for _, row in df.iterrows():
        insert_record_given_symbol(cursor, table_name, symbol, columns, row)


def insert_record_given_symbol(cursor, table_name, symbol, columns, values):
    placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for each column

    if "company_id" in columns:
        raise ValueError("company_id exists in columns, this is a foreign key")

    command = (f"INSERT INTO {table_name} ({'company_id, ' + ', '.join(columns)}) "
               f"VALUES ((SELECT c.id FROM company c WHERE c.symbol = '{symbol}'), {placeholders})")
    try:
        cursor.execute(command, tuple(values))
    except psycopg2.errors.NotNullViolation:
        cursor.execute(f"INSERT INTO company (symbol) VALUES ('{symbol}')")
        cursor.execute(command)


def update_column_target_symbol(table_name, target_column, value_to_set, target_symbol):
    command = f"UPDATE {table_name} SET {target_column} = %s WHERE symbol = %s"
    config = load_config()  # Load your database configuration
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(command, (value_to_set if type(value_to_set) != np.bool_ else bool(value_to_set), target_symbol))
                updated_row_count = cur.rowcount
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return updated_row_count
