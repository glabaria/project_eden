import pandas as pd
import certifi
import json
import time
import os
import datetime
from enum import Enum
from typing import Optional
from urllib.request import urlopen

from db.utils import (load_config, connect, insert_record, insert_records_from_df, update_column_target_symbol,
                         insert_records_from_df_given_symbol)
from db.create_tables import (DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE, DEFAULT_SHARES_COLUMNS_TO_TYPE,
                                 FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES,
                              POSTGRES_COLUMN_NAMES_TO_FMP_COLUMN_NAMES, DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE,
                              DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE, DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE)


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


dataset_to_table_name_quarter = {
    Datasets.INCOME_STATEMENT: "income_statement_quarter",
    Datasets.BALANCE_SHEET_STATEMENT: "balance_sheet_quarter",
    Datasets.CASH_FLOW_STATEMENT: "cash_flow_statement_quarter",
}


dataset_to_table_columns = {
    Datasets.INCOME_STATEMENT: list(DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.BALANCE_SHEET_STATEMENT: list(DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.CASH_FLOW_STATEMENT: list(DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.ENTERPRISE_VALUES: list(DEFAULT_SHARES_COLUMNS_TO_TYPE.keys())
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


def add_datasets_to_db(connection, symbol, datasets, **kwargs):

    datasets = Datasets if datasets is None else datasets
    period = kwargs.get("period", "fy")
    dataset_to_table_name_to_use = dataset_to_table_name if period == "fy" else dataset_to_table_name_quarter

    try:
        with connection.cursor() as cursor:

            cursor.execute(f"SELECT * FROM company WHERE symbol = '{symbol}'")
            # TODO: DEBUG
            is_exist = cursor.fetchone()
            # if is_exist:
            #     print(f"--{symbol} already inserted into db.  Checking for diffs.")
            #     for dataset in datasets:
            #         dataset_df = gather_dataset(symbol, dataset.value, key, **kwargs)
            #         cursor.execute(f"SELECT * FROM {dataset_to_table_name_to_use[dataset]} WHERE symbol = '{symbol}'")
            #         existing_df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description])
            #         columns_to_compare = dataset_to_table_columns[dataset]
            #         columns_to_compare = [x for x in columns_to_compare if x not in ["id", "company_id"]]
            #         columns_to_compare_lower = [x.lower() for x in columns_to_compare]
            #         dataset_df.rename(columns={x: y for x, y in zip(columns_to_compare, columns_to_compare_lower)}, inplace=True)
            #         dataset_df["calendaryear"] = dataset_df["calendaryear"].apply(lambda x: int(x))
            #         diffs = dataset_df[columns_to_compare_lower].merge(existing_df[columns_to_compare_lower], on=["calendaryear", "period"], how="outer", indicator=True).loc[lambda x: x['_merge'] == 'left_only']
            #
            #         if not diffs.empty:
            #             print(f"--Diffs found for {symbol} in {dataset_to_table_name_to_use[dataset]} table.")
            #             diffs.drop(columns=[x for x in diffs.columns if x[-2:] == "_y"] + ["_merge"], inplace=True)
            #             diffs.rename(columns={x: x[:-2] if x[-2:] == "_x" else x for x in diffs.columns}, inplace=True)
            #             print(diffs)
            #             print(f"--Updating {symbol} for {dataset_to_table_name_to_use[dataset]} table.")
            #             columns = diffs.columns.values
            #             for _, row in diffs.iterrows():
            #                 update_column_target_symbol(dataset_to_table_name_to_use[dataset], columns, row, symbol,
            #                                             cursor=cursor)
            # else:
            #     print(f"--Inserting {symbol} to company table.")
            #     insert_record(cursor, "company", ["symbol"], [symbol])
            for dataset in datasets:
                print(f"--Inserting {symbol} for {dataset_to_table_name_to_use[dataset]} table.")
                dataset_df = gather_dataset(symbol, dataset.value, key, **kwargs)
                insert_records_from_df(cursor, dataset_df, dataset_to_table_name_to_use[dataset])
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

        with open('/db/db/company_tickers.json') as user_file:
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
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "company_tickers.json")) as user_file:
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
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "company_tickers.json")) as user_file:
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


def main_quarter(start_from_symbol=None):
    db_config = load_config()

    with connect(db_config) as connection:
        if connection:
            print("Connected successfully!")
        else:
            raise ValueError(f"Failed to connect to db: {db_config}")

        # get latest from https://www.sec.gov/files/company_tickers.json
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "company_tickers.json")) as user_file:
            file_contents = user_file.read()
            ticker_dict = json.loads(file_contents)

        counter = 0
        api_limit_per_min = 300
        start_flag = True if start_from_symbol is None else False
        start_time = time.time()

        for value_dict in ticker_dict.values():
            current_time = time.time()
            elapsed_time = current_time - start_time

            if counter >= api_limit_per_min:
                sleep_time = 60 - elapsed_time
                if sleep_time > 0:
                    time.sleep(sleep_time)
                counter = 0
                start_time = time.time()

            symbol = value_dict["ticker"]
            if symbol != "AAPL":  # TODO: DEBUG ONLY
                continue
            if not start_flag and start_from_symbol is not None and start_from_symbol == symbol:
                start_flag = True
            elif not start_flag and start_from_symbol is not None and start_from_symbol != symbol:
                print(f"Have not yet encountered {start_from_symbol}, skipping {symbol}.")
                continue

            if start_flag:
                print(f"Processing {symbol}")
                add_datasets_to_db(connection, symbol, datasets=[Datasets.INCOME_STATEMENT, Datasets.CASH_FLOW_STATEMENT, Datasets.BALANCE_SHEET_STATEMENT], period="quarter")
                counter += 1


if __name__ == "__main__":
    with open("key.txt") as f:
        key = f.readlines()[0]

    symbols_with_failure = []

    # main(start_from_symbol=None)
    # print(f"The following symbols failed: {symbols_with_failure}")

    # add_full_company_information(start_from_symbol="FCX")
    # add_shares()

    main_quarter()
    print(f"The following symbols failed: {symbols_with_failure}")
