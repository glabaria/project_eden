import pandas as pd
import certifi
import json
import time
import os
import datetime
import ssl
from enum import Enum
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from db.utils import (
    load_config,
    connect,
    insert_records_from_df,
)
from db.create_tables import (
    DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE,
    DEFAULT_SHARES_COLUMNS_TO_TYPE,
    FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES,
    DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE,
    DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE,
    DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE,
    postgres_type_to_python_type,
)


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
    PROFILE = PROFILE


dataset_to_table_name = {
    Datasets.INCOME_STATEMENT: "income_statement_fy",
    Datasets.BALANCE_SHEET_STATEMENT: "balance_sheet_fy",
    Datasets.CASH_FLOW_STATEMENT: "cash_flow_statement_fy",
    Datasets.ENTERPRISE_VALUES: "shares_fy",
}


dataset_to_table_name_quarter = {
    Datasets.INCOME_STATEMENT: "income_statement_quarter",
    Datasets.BALANCE_SHEET_STATEMENT: "balance_sheet_quarter",
    Datasets.CASH_FLOW_STATEMENT: "cash_flow_statement_quarter",
    Datasets.PROFILE: "company",
}


dataset_to_table_columns = {
    Datasets.INCOME_STATEMENT: list(DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.BALANCE_SHEET_STATEMENT: list(DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.CASH_FLOW_STATEMENT: list(DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()),
    Datasets.ENTERPRISE_VALUES: list(DEFAULT_SHARES_COLUMNS_TO_TYPE.keys()),
    Datasets.PROFILE: list(DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE.keys()),
}


def get_jsonparsed_data(
    dataset_name: str,
    ticker: str,
    key: str,
    base_url: str = "https://financialmodelingprep.com/api/v3",
    **kwargs,
) -> dict:
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
    context = ssl.create_default_context(cafile=certifi.where())
    response = urlopen(url, context=context)
    data = response.read().decode("utf-8")
    return json.loads(data)


def gather_dataset(
    ticker: str, dataset: str, key: str, period: Optional[str] = None, **kwargs
) -> pd.DataFrame:
    kwargs_to_use = (
        dict(period=period, **kwargs)
        if period is not None
        else kwargs
        if kwargs is not None
        else {}
    )
    json_data = get_jsonparsed_data(dataset, ticker, key, **kwargs_to_use)
    return pd.DataFrame.from_records(json_data)


def add_datasets_to_db(connection, symbol, datasets, **kwargs):
    datasets = Datasets if datasets is None else datasets
    period = kwargs.get("period", "fy")
    dataset_to_table_name_to_use = (
        dataset_to_table_name if period == "fy" else dataset_to_table_name_quarter
    )

    try:
        with connection.cursor() as cursor:
            for dataset in datasets:
                table_name = dataset_to_table_name_to_use[dataset]
                print(f"--Processing {symbol} for {table_name} table.")

                # Fetch new data from API
                new_data_df = gather_dataset(symbol, dataset.value, key, **kwargs)

                if new_data_df.empty:
                    print(f"--No new data found for {symbol} in {table_name}, skipping.")
                    continue

                # Standardize column names to match database
                new_data_df.rename(columns=FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES, inplace=True)
                columns_to_compare = [
                    FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.get(col, col)
                    for col in dataset_to_table_columns[dataset]
                    if col not in ["id", "company_id"]
                ]
                columns_to_compare = list(set(columns_to_compare))

                # Convert calendaryear to int for proper comparison
                if "calendaryear" in new_data_df.columns:
                    new_data_df["calendaryear"] = new_data_df["calendaryear"].astype(int)

                # Fetch existing data from database
                cursor.execute(f"SELECT * FROM {table_name} WHERE symbol = '{symbol}'")
                existing_records = cursor.fetchall()

                if existing_records:
                    # Convert existing records to DataFrame
                    existing_df = pd.DataFrame(
                        existing_records, columns=[desc[0] for desc in cursor.description]
                    )

                    # Identify records to update or insert
                    if dataset == Datasets.PROFILE:
                        merge_keys = ["symbol"]
                    else:
                        merge_keys = (
                            ["calendaryear", "period"]
                            if "period" in new_data_df.columns
                            else ["calendaryear"]
                        )
                    comparison = new_data_df.merge(
                        existing_df[columns_to_compare], on=merge_keys, how="left", indicator=True
                    )

                    # Handle new records (left_only)
                    new_records = comparison[comparison["_merge"] == "left_only"]
                    if not new_records.empty:
                        new_records_str = "\n".join(
                            [
                                f"{', '.join([f'{key}={row[key]}' for key in merge_keys])}"
                                for _, row in new_records.iterrows()
                            ]
                        )
                        print(
                            f"--Found {len(new_records)} new records: {new_records_str}\nfor {symbol} in {table_name}"
                        )
                        new_records_clean = new_records.rename(
                            columns={f"{col}_x": col for col in columns_to_compare}
                        )[columns_to_compare]
                        insert_records_from_df(cursor, new_records_clean, table_name)

                    # Handle updates (both present but different values)
                    updates = comparison[comparison["_merge"] == "both"]
                    for _, row in updates.iterrows():
                        update_needed = False
                        update_values = {}

                        for col in columns_to_compare:
                            if col in merge_keys:
                                continue
                            new_val = row[f"{col}_x"] if f"{col}_x" in row else row[col]
                            old_val = row[f"{col}_y"] if f"{col}_y" in row else None

                            # Replace the selected line with a more comprehensive comparison
                            if pd.notna(new_val) and pd.notna(old_val):
                                if isinstance(new_val, (int, float)) and isinstance(
                                    old_val, (int, float)
                                ):
                                    # For numerical values, allow small differences
                                    if abs(new_val - old_val) > 1e-3:
                                        update_needed = True
                                        update_values[col] = new_val
                                elif isinstance(old_val, datetime.date) and isinstance(
                                    new_val, str
                                ):
                                    # Convert string to date for comparison
                                    try:
                                        # Try different date formats with and without time components
                                        if " " in new_val:  # Check if there's a time component
                                            new_date = datetime.datetime.strptime(
                                                new_val, "%Y-%m-%d %H:%M:%S"
                                            ).date()
                                        else:
                                            new_date = datetime.datetime.strptime(
                                                new_val, "%Y-%m-%d"
                                            ).date()
                                        # Handle pandas Timestamp objects by converting to date
                                        old_date = old_val
                                        if hasattr(old_val, "date") and callable(
                                            getattr(old_val, "date")
                                        ):
                                            old_date = old_val.date()

                                        # Now compare the date portions only
                                        if new_date != old_date:
                                            update_needed = True
                                            # Store the date object rather than the string
                                            update_values[col] = new_date
                                    except ValueError:
                                        # If string format is invalid, consider them different
                                        update_needed = True
                                        update_values[col] = new_val
                                elif type(new_val) != type(old_val):
                                    # If types are different, attempt to reconcile them and compare
                                    try:
                                        new_val = postgres_type_to_python_type(col)(new_val)
                                        if new_val != old_val:
                                            update_needed = True
                                            update_values[col] = new_val
                                    except ValueError:
                                        # If reconciliation fails, consider them different
                                        update_needed = True
                                        update_values[col] = new_val
                                elif new_val != old_val:
                                    # For strings, booleans, dates, etc. use exact comparison
                                    update_needed = True
                                    update_values[col] = new_val
                            elif pd.notna(new_val) and pd.isna(old_val):
                                # If old value is null but new value exists
                                update_needed = True
                                update_values[col] = new_val

                        if update_needed:
                            records_updated_str = " and ".join(
                                [f"{key} = '{row[key]}'" for key in merge_keys]
                            )
                            updated_records_str = ",\n".join(
                                [
                                    f"{key} = '{row[key + '_y']}' -> {key} = '{row[key + '_x']}'"
                                    for key in update_values.keys()
                                ]
                            )
                            print(
                                f"--Updating records:\n{updated_records_str}\nwhere\n{records_updated_str} for {symbol} in {table_name}\n"
                            )
                            where_clause = " AND ".join(
                                [f"{key} = '{row[key]}'" for key in merge_keys]
                            )
                            set_clause = ", ".join([f"{col} = %s" for col in update_values.keys()])
                            update_sql = f"""
                                UPDATE {table_name} 
                                SET {set_clause}
                                WHERE symbol = '{symbol}' AND {where_clause}
                            """
                            cursor.execute(update_sql, list(update_values.values()))
                else:
                    # If no existing records, insert all new data
                    print(f"--Inserting new records for {symbol} in {table_name}")
                    insert_records_from_df(cursor, new_data_df[columns_to_compare], table_name)

        connection.commit()
        print(f"{symbol} processing complete.")
        print("")

    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        import traceback

        traceback.print_exc()
        connection.rollback()
        symbols_with_failure.append(symbol)


def get_company_tickers():
    """Fetch the latest company tickers JSON from SEC website"""
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        'User-Agent': '123 (123@gmail.com)',  # FIXME: add to config
    }

    try:
        req = Request(url, headers=headers)
        context = ssl.create_default_context(cafile=certifi.where())
        response = urlopen(req, context=context)
        data = response.read().decode("utf-8")
        return json.loads(data)
    except HTTPError as e:
        print(f"Error fetching company tickers: {e}")
        print("Falling back to local file...")
        # Fallback to local file
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "company_tickers.json")) as user_file:
            file_contents = user_file.read()
            return json.loads(file_contents)


def main_quarter(start_from_symbol=None, db_init_file="database_dev_v2.ini", section="postgresql"):
    db_config = load_config(filename=db_init_file, section=section)

    with connect(db_config) as connection:
        if connection:
            print("Connected successfully!")
        else:
            raise ValueError(f"Failed to connect to db: {db_config}")

        ticker_dict = get_company_tickers()

        counter = 0
        api_limit_per_min = 300
        start_flag = True if start_from_symbol is None else False
        start_time = time.time()

        for value_dict in ticker_dict.values():
            # Check if we need to throttle API calls
            current_time = time.time()
            elapsed_time = current_time - start_time

            # If we've reached the limit but less than a minute has passed
            if counter >= api_limit_per_min and elapsed_time < 60:
                sleep_time = 60 - elapsed_time
                if sleep_time > 0:
                    print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                counter = 0
                start_time = time.time()
            # If a minute or more has passed, reset counter and timer
            elif elapsed_time >= 60:
                counter = 0
                start_time = time.time()

            symbol = value_dict["ticker"]
            if not start_flag and start_from_symbol is not None and start_from_symbol == symbol:
                start_flag = True
            elif not start_flag and start_from_symbol is not None and start_from_symbol != symbol:
                print(f"Have not yet encountered {start_from_symbol}, skipping {symbol}.")
                continue

            if start_flag:
                print(f"Processing {symbol}")
                add_datasets_to_db(
                    connection,
                    symbol,
                    datasets=[
                        Datasets.PROFILE,
                        Datasets.INCOME_STATEMENT,
                        Datasets.CASH_FLOW_STATEMENT,
                        Datasets.BALANCE_SHEET_STATEMENT,
                    ],
                    period="quarter",
                )
                counter += 1


if __name__ == "__main__":
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "key.txt")) as f:
        key = f.readlines()[0]

    symbols_with_failure = []

    db_init_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database_v2.ini")
    main_quarter(start_from_symbol=None, db_init_file=db_init_file, section="postgresql")
    print(f"The following symbols failed: {symbols_with_failure}")
