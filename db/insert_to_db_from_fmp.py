import pandas as pd
import certifi
import json
import os
import psycopg2
import time
from enum import Enum
from typing import Optional
from urllib.request import urlopen

from db.utils import (load_config, connect, insert_record, insert_records_from_df, update_column_target_symbol,
                      insert_record_given_symbol, insert_records_from_df_given_symbol)
from db.create_tables import (DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE, DEFAULT_SHARES_COLUMNS_TO_TYPE,
                              FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES, POSTGRES_COLUMN_NAMES_TO_FMP_COLUMN_NAMES)


INCOME_STATEMENT = "income-statement"
BALANCE_SHEET_STATEMENT = "balance-sheet-statement"
CASH_FLOW_STATEMENT = "cash-flow-statement"
PROFILE = "profile"
ENTERPRISE_VALUES = "enterprise-values"


class Datasets(Enum):
    INCOME_STATEMENT = INCOME_STATEMENT
    BALANCE_SHEET_STATEMENT = BALANCE_SHEET_STATEMENT
    CASH_FLOW_STATEMENT = CASH_FLOW_STATEMENT
    ENTERPRISE_VALUES = ENTERPRISE_VALUES


dataset_to_table_name = {
    Datasets.INCOME_STATEMENT: "income_statement_fy",
    Datasets.BALANCE_SHEET_STATEMENT: "balance_sheet_fy",
    Datasets.CASH_FLOW_STATEMENT: "cash_flow_statement_fy",
    Datasets.ENTERPRISE_VALUES: "shares_fy"
}


# def insert_to_db_from_fmp(table_name, columns, values):
#     placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for each column
#     command = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
#     cursor.execute(command, tuple(values))


def get_jsonparsed_data(dataset_name: str, ticker: str, key: str,
                        base_url: str = "https://financialmodelingprep.com/api/v3",
                        **kwargs) -> dict:
    """
    Receive the content of from a url of the form f"{base_url}/{dataset_name}/{ticker}?apikey={key}".

    Parameters
    ----------
    dataset_name
    base_url : str
    ticker
    key
    **kwargs:

    Returns
    -------
    dict
    """
    url = f"{base_url}/{dataset_name}/{ticker}?apikey={key}"
    for key, value in kwargs.items():
        url += f"&{key}={value}"
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


def gather_dataset(ticker: str, dataset: str, key: str, period: Optional[str] = None, **kwargs) -> pd.DataFrame:
    kwargs_to_use = dict(period=period, **kwargs) if period is not None else kwargs if kwargs is not None else {}
    json_data = get_jsonparsed_data(dataset, ticker, key, **kwargs_to_use)
    return pd.DataFrame.from_records(json_data)


def add_datasets_to_db(connection, symbol):

    try:
        with connection.cursor() as cursor:

            cursor.execute(f"SELECT * FROM company WHERE symbol = '{symbol}'")
            is_exist = cursor.fetchall()
            if is_exist:
                print(f"--{symbol} already inserted into db.  Skipping...")
                return

            print(f"--Inserting {symbol} to company table.")
            insert_record(cursor, "company", ["symbol"], [symbol])
            for dataset in Datasets:
                print(f"--Inserting {symbol} for {dataset_to_table_name[dataset]} table.")
                dataset_df = gather_dataset(symbol, dataset.value, key)
                insert_records_from_df(cursor, dataset_df, dataset_to_table_name[dataset])
        connection.commit()
        print(f"{symbol} insertion complete.")
        print("")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        connection.rollback()
        symbols_with_failure.append(symbol)


def add_dataset_to_db(connection, symbol, dataset, table_name, columns_to_add):
    try:
        with connection.cursor() as cursor:

            cursor.execute(f"SELECT id FROM company WHERE symbol = '{symbol}'")
            company_id = cursor.fetchall()
            if company_id is None:
                print(f"--Inserting {symbol} to company table.")
                insert_record(cursor, "company", ["symbol"], [symbol])

            print(f"--Inserting {symbol} for {table_name} table.")
            dataset_df = gather_dataset(symbol, dataset.value, key)
            dataset_df = dataset_df[[POSTGRES_COLUMN_NAMES_TO_FMP_COLUMN_NAMES.get(x, x) for x in columns_to_add]]
            dataset_df.rename(columns=FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES, inplace=True)
            insert_records_from_df_given_symbol(cursor, dataset_df, table_name, symbol)
        connection.commit()
        print(f"{symbol} insertion complete.")
        print("")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        connection.rollback()
        symbols_with_failure.append(symbol)


def add_full_company_information(start_from_symbol=None):
    db_config = load_config()

    with connect(db_config) as connection:
        if connection:
            print("Connected successfully!")
        else:
            raise ValueError(f"Failed to connect to db: {db_config}")

        with open('C:/Users/georg/PycharmProjects/project_eden/db/company_tickers.json') as user_file:
            file_contents = user_file.read()
            ticker_dict = json.loads(file_contents)

        counter = 1
        limit_per_min = 300 / 1
        start_flag = True if start_from_symbol is None else False
        for value_dict in ticker_dict.values():
            if counter >= limit_per_min / 2:
                time.sleep(60)
                counter = 1

            symbol = value_dict["ticker"]
            if not start_flag and start_from_symbol is not None and start_from_symbol == symbol:
                start_flag = True
            elif not start_flag and start_from_symbol is not None and start_from_symbol != symbol:
                print(f"Have not yet encountered {start_from_symbol}, skipping {symbol}.")
                continue

            if start_flag:
                print(f"Processing {symbol}")
                try:
                    dataset_df = gather_dataset(symbol, PROFILE, key)
                    for column in dataset_df.columns.values:
                        if column.lower() in DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE and column not in ["symbol", "id"]:
                            update_column_target_symbol("company", column.lower(), dataset_df[column].values[0], symbol)
                except:
                    connection.rollback()
                    symbols_with_failure.append(symbol)
                counter += 1


def add_shares(start_from_symbol=None):
    db_config = load_config()

    with connect(db_config) as connection:
        if connection:
            print("Connected successfully!")
        else:
            raise ValueError(f"Failed to connect to db: {db_config}")

        # get latest from https://www.sec.gov/files/company_tickers.json
        with open('C:/Users/georg/PycharmProjects/project_eden/db/company_tickers.json') as user_file:
            file_contents = user_file.read()
            ticker_dict = json.loads(file_contents)

        counter = 1
        limit_per_min = 300
        start_flag = True if start_from_symbol is None else False
        for value_dict in ticker_dict.values():
            if counter >= limit_per_min:
                time.sleep(60)
                counter = 1

            symbol = value_dict["ticker"]
            if not start_flag and start_from_symbol is not None and start_from_symbol == symbol:
                start_flag = True
            elif not start_flag and start_from_symbol is not None and start_from_symbol != symbol:
                print(f"Have not yet encountered {start_from_symbol}, skipping {symbol}.")
                continue

            if start_flag:
                print(f"Processing {symbol}")
                add_dataset_to_db(connection, symbol, Datasets.ENTERPRISE_VALUES, "shares_fy",
                                  [x for x in DEFAULT_SHARES_COLUMNS_TO_TYPE if x != "company_id"])
                counter += 1


def main(start_from_symbol=None):
    db_config = load_config()

    with connect(db_config) as connection:
        if connection:
            print("Connected successfully!")
        else:
            raise ValueError(f"Failed to connect to db: {db_config}")

        # get latest from https://www.sec.gov/files/company_tickers.json
        with open('C:/Users/georg/PycharmProjects/project_eden/db/company_tickers.json') as user_file:
            file_contents = user_file.read()
            ticker_dict = json.loads(file_contents)

        counter = 1
        limit_per_min = 300 / len(Datasets)
        start_flag = True if start_from_symbol is None else False
        for value_dict in ticker_dict.values():
            if counter >= limit_per_min:
                time.sleep(60)
                counter = 1

            symbol = value_dict["ticker"]
            if not start_flag and start_from_symbol is not None and start_from_symbol == symbol:
                start_flag = True
            elif not start_flag and start_from_symbol is not None and start_from_symbol != symbol:
                print(f"Have not yet encountered {start_from_symbol}, skipping {symbol}.")
                continue

            if start_flag:
                print(f"Processing {symbol}")
                add_datasets_to_db(connection, symbol)
                counter += 1


if __name__ == "__main__":
    with open("./key.txt") as f:
        key = f.readlines()[0]

    symbols_with_failure = []

    # main(start_from_symbol=None)
    # print(f"The following symbols failed: {symbols_with_failure}")

    # add_full_company_information(start_from_symbol="FCX")
    add_shares()
    print(f"The following symbols failed: {symbols_with_failure}")
