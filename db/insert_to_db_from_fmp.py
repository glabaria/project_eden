import pandas as pd
import certifi
import json
import os
import psycopg2
import time
from enum import Enum
from typing import Optional
from urllib.request import urlopen

from db.utils import load_config, connect, insert_record, insert_records_from_df


INCOME_STATEMENT = "income-statement"
BALANCE_SHEET_STATEMENT = "balance-sheet-statement"
CASH_FLOW_STATEMENT = "cash-flow-statement"


class Datasets(Enum):
    INCOME_STATEMENT = INCOME_STATEMENT
    BALANCE_SHEET_STATEMENT = BALANCE_SHEET_STATEMENT
    CASH_FLOW_STATEMENT = CASH_FLOW_STATEMENT


dataset_to_table_name = {
    Datasets.INCOME_STATEMENT: "income_statement_fy",
    Datasets.BALANCE_SHEET_STATEMENT: "balance_sheet_fy",
    Datasets.CASH_FLOW_STATEMENT: "cash_flow_statement_fy"
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


def main(start_from_symbol=None):
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
        limit_per_min = 300 / len(Datasets)
        start_flag = True if start_from_symbol is None else False
        for value_dict in ticker_dict.values():
            if counter == limit_per_min:
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

    main(start_from_symbol="STLA")
