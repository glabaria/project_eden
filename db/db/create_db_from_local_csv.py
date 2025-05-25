import psycopg2
import pandas as pd
import os

from db.utils import load_config
from db.create_tables import (
    create_income_statement_table,
    create_company_table,
    create_balance_sheet_table,
    create_cash_flow_statement_table,
)

LOCAL_BASE_FILEPATH = "C:/project_eden_shared/all_financials_sp500"


def connect(config):
    try:
        conn = psycopg2.connect(**config)
        # Now you can use the connection (e.g., create a cursor and execute queries)
        return conn
    except Exception as e:
        print(f"Error: {e}")


def insert_record(cursor, table_name, columns, values):
    placeholders = ", ".join(["%s"] * len(columns))  # Create placeholders for each column
    command = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.execute(command, tuple(values))


def insert_record_with_company_id(cursor, table_name, columns, values):
    placeholders = ", ".join(["%s"] * len(columns))  # Create placeholders for each column

    if "company_id" in columns:
        raise ValueError("company_id exists in columns, this is a foreign key")
    if "symbol" not in columns:
        raise ValueError("symbol not found in columns")
    symbol_value = [values.values[ind] for ind, column in enumerate(columns) if column == "symbol"]
    symbol_value = symbol_value[0]
    command = (
        f"INSERT INTO {table_name} ({'company_id, ' + ', '.join(columns)}) "
        f"VALUES ((SELECT c.id FROM company c WHERE c.symbol = '{symbol_value}'), {placeholders})"
    )
    try:
        cursor.execute(command, tuple(values))
    except psycopg2.errors.NotNullViolation:
        cursor.execute(f"INSERT INTO company (symbol) VALUES ('{symbol_value}')")
        cursor.execute(command)


def insert_records_from_df(cursor, df: pd.DataFrame, table_name):
    columns = df.columns.values
    for _, row in df.iterrows():
        insert_record_with_company_id(cursor, table_name, columns, row)


def load_csvs_and_insert(csv_folder_path, cursor, table_name):
    for root, dirs, files in os.walk(csv_folder_path):
        for file_name in files:
            if file_name.endswith(".csv"):
                file_path = os.path.join(root, file_name)
                try:
                    df = pd.read_csv(file_path)
                except Exception as e:
                    print(f"Error reading {file_path}")
                    continue
                insert_records_from_df(cursor, df, table_name)


def main():
    db_config = load_config()
    connection = connect(db_config)
    if connection:
        print("Connected successfully!")
    else:
        raise ValueError(f"Failed to connect to db: {db_config}")

    # cursor = connection.cursor()
    # cursor.execute("SELECT * FROM temp WHERE is_longdog = 'True';")
    #
    # LongDogRow = namedtuple('LongDogRow', [desc[0] for desc in cursor.description])
    #
    # rows = cursor.fetchall()  # Fetch all rows
    # named_rows = [LongDogRow(*row) for row in rows]
    # for row in named_rows:
    #     print(row)

    create_company_table(connection, table_name="company")
    create_income_statement_table(
        connection,
        table_name="income_statement_fy",
        foreign_key_ref_tuple=("company_id", "company", "id"),
    )
    create_balance_sheet_table(
        connection,
        table_name="balance_sheet_fy",
        foreign_key_ref_tuple=("company_id", "company", "id"),
    )
    create_cash_flow_statement_table(
        connection,
        table_name="cash_flow_statement_fy",
        foreign_key_ref_tuple=("company_id", "company", "id"),
    )

    cursor = connection.cursor()
    for root, dirs, files in os.walk(os.path.join(LOCAL_BASE_FILEPATH, "balance_sheets")):
        for file_name in files:
            if file_name.endswith(".csv"):
                try:
                    df = pd.read_csv(
                        os.path.join(LOCAL_BASE_FILEPATH, "balance_sheets", file_name),
                        usecols=["symbol"],
                    )
                except:
                    print(f"error with {file_name}")
                    continue
                insert_record(cursor, "company", ["symbol"], [df["symbol"].values[0]])
    load_csvs_and_insert(
        os.path.join(LOCAL_BASE_FILEPATH, "income_statements"), cursor, "income_statement_fy"
    )
    load_csvs_and_insert(
        os.path.join(LOCAL_BASE_FILEPATH, "balance_sheets"), cursor, "balance_sheet_fy"
    )
    load_csvs_and_insert(
        os.path.join(LOCAL_BASE_FILEPATH, "cash_flows"), cursor, "cash_flow_statement_fy"
    )
    cursor.close()

    connection.commit()
    connection.close()


def get_current_market_cap():
    pass


if __name__ == "__main__":
    main()
