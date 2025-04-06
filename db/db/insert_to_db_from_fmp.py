import pandas as pd
import certifi
import json
import time
import os
import datetime
import ssl
import math
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
    dataset_name : str
        The name of the dataset to retrieve
    ticker : str
        The stock ticker symbol
    key : str
        The API key for authentication
    base_url : str, default="https://financialmodelingprep.com/api/v3"
        The base URL for the API
    **kwargs
        Additional query parameters to include in the URL

    Returns
    -------
    dict
        The parsed JSON response from the API
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
    """
    Gather dataset from the financial API and convert to DataFrame.

    Parameters
    ----------
    ticker : str
        The stock ticker symbol
    dataset : str
        The dataset name to retrieve
    key : str
        The API key for authentication
    period : str, optional
        The period to retrieve (e.g., "quarter" or "fy")
    **kwargs
        Additional parameters to pass to the API

    Returns
    -------
    pd.DataFrame
        DataFrame containing the retrieved data
    """
    kwargs_to_use = (
        dict(period=period, **kwargs)
        if period is not None
        else kwargs
        if kwargs is not None
        else {}
    )
    json_data = get_jsonparsed_data(dataset, ticker, key, **kwargs_to_use)
    return pd.DataFrame.from_records(json_data)


def add_datasets_to_db(connection, symbol, datasets, key=None, failure_list=None, **kwargs):
    """
    Add datasets for a symbol to the database.

    Parameters
    ----------
    connection
        Database connection
    symbol : str
        Stock symbol to process
    datasets : list
        List of datasets to process
    key : str, optional
        API key for the financial data provider
    failure_list : list, optional
        List to append failed symbols to
    **kwargs
        Additional arguments for dataset gathering
    """
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
                columns_to_compare = get_columns_to_compare(dataset)

                # Convert calendaryear to int for proper comparison
                if "calendaryear" in new_data_df.columns:
                    new_data_df["calendaryear"] = new_data_df["calendaryear"].astype(int)

                process_dataset(cursor, symbol, table_name, new_data_df, columns_to_compare, dataset)

        connection.commit()
        print(f"{symbol} processing complete.")
        print("")

    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        import traceback

        traceback.print_exc()
        connection.rollback()
        if failure_list is not None:
            failure_list.append(symbol)


def get_columns_to_compare(dataset):
    """
    Get the columns to compare for a given dataset.

    Parameters
    ----------
    dataset
        The dataset to get columns for

    Returns
    -------
    list
        A list of column names to compare
    """
    columns_to_compare = [
        FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.get(col, col)
        for col in dataset_to_table_columns[dataset]
        if col not in ["id", "company_id"]
    ]
    return list(set(columns_to_compare))


def process_dataset(cursor, symbol, table_name, new_data_df, columns_to_compare, dataset):
    """
    Process a dataset by either updating existing records or inserting new ones.

    Parameters
    ----------
    cursor
        Database cursor
    symbol : str
        Stock symbol being processed
    table_name : str
        Name of the database table
    new_data_df : DataFrame
        DataFrame containing new data
    columns_to_compare : list
        List of columns to compare
    dataset
        The dataset being processed
    """
    # Fetch existing data from database
    cursor.execute(f"SELECT * FROM {table_name} WHERE symbol = '{symbol}'")
    existing_records = cursor.fetchall()

    if existing_records:
        process_existing_records(cursor, symbol, table_name, new_data_df,
                                columns_to_compare, existing_records, dataset)
    else:
        # If no existing records, insert all new data
        print(f"--Inserting new records for {symbol} in {table_name}")
        insert_records_from_df(cursor, new_data_df[columns_to_compare], table_name)


def process_existing_records(cursor, symbol, table_name, new_data_df,
                           columns_to_compare, existing_records, dataset):
    """
    Process records when there are existing entries in the database.

    Parameters
    ----------
    cursor
        Database cursor
    symbol : str
        Stock symbol being processed
    table_name : str
        Name of the database table
    new_data_df : DataFrame
        DataFrame containing new data
    columns_to_compare : list
        List of columns to compare
    existing_records : list
        Existing records from the database
    dataset
        The dataset being processed
    """
    # Convert existing records to DataFrame
    existing_df = pd.DataFrame(
        existing_records, columns=[desc[0] for desc in cursor.description]
    )

    # Identify records to update or insert
    merge_keys = get_merge_keys(dataset, new_data_df)

    comparison = new_data_df.merge(
        existing_df[columns_to_compare], on=merge_keys, how="left", indicator=True
    )

    # Handle new records (left_only)
    process_new_records(cursor, symbol, table_name, comparison,
                       columns_to_compare, merge_keys)

    # Handle updates (both present but different values)
    process_updates(cursor, symbol, table_name, comparison,
                   columns_to_compare, merge_keys)


def get_merge_keys(dataset, new_data_df):
    """
    Determine the merge keys based on the dataset.

    Parameters
    ----------
    dataset
        The dataset being processed
    new_data_df : DataFrame
        DataFrame containing new data

    Returns
    -------
    list
        A list of column names to use as merge keys
    """
    if dataset == Datasets.PROFILE:
        return ["symbol"]
    else:
        return ["calendaryear", "period"] if "period" in new_data_df.columns else ["calendaryear"]


def process_new_records(cursor, symbol, table_name, comparison, columns_to_compare, merge_keys):
    """
    Process records that exist in the new data but not in the database.

    Parameters
    ----------
    cursor
        Database cursor
    symbol : str
        Stock symbol being processed
    table_name : str
        Name of the database table
    comparison : DataFrame
        DataFrame containing comparison results
    columns_to_compare : list
        List of columns to compare
    merge_keys : list
        List of columns used as merge keys
    """
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


def process_updates(cursor, symbol, table_name, comparison, columns_to_compare, merge_keys):
    """
    Process records that exist in both datasets but may have different values.

    Parameters
    ----------
    cursor
        Database cursor
    symbol : str
        Stock symbol being processed
    table_name : str
        Name of the database table
    comparison : DataFrame
        DataFrame containing comparison results
    columns_to_compare : list
        List of columns to compare
    merge_keys : list
        List of columns used as merge keys
    """
    updates = comparison[comparison["_merge"] == "both"]
    for _, row in updates.iterrows():
        update_needed = False
        update_values = {}

        for col in columns_to_compare:
            if col in merge_keys:
                continue

            new_val = row[f"{col}_x"] if f"{col}_x" in row else row[col]
            old_val = row[f"{col}_y"] if f"{col}_y" in row else None

            if should_update_value(new_val, old_val, col):
                update_needed = True
                update_values[col] = new_val

        if update_needed:
            apply_updates(cursor, symbol, table_name, row, update_values, merge_keys)


def should_update_value(new_val, old_val, col):
    """
    Determine if a value should be updated based on comparison logic.

    Parameters
    ----------
    new_val
        The new value from the API
    old_val
        The existing value in the database
    col : str
        The column name

    Returns
    -------
    bool
        True if the value should be updated, False otherwise
    """
    # If both values are present, compare them
    if pd.notna(new_val) and pd.notna(old_val):
        # Handle date comparisons
        if isinstance(new_val, str) and isinstance(old_val, (datetime.date, datetime.datetime)):
            try:
                new_date = datetime.datetime.strptime(new_val, "%Y-%m-%d" if " " not in new_val else "%Y-%m-%d %H:%M:%S").date()
                old_date = old_val
                if hasattr(old_val, "date") and callable(getattr(old_val, "date")):
                    old_date = old_val.date()
                return new_date != old_date
            except ValueError:
                return True
        # Handle type differences
        elif type(new_val) != type(old_val):
            try:
                new_val = postgres_type_to_python_type(col)(new_val)
                return new_val != old_val
            except ValueError:
                return True
        # Handle numeric comparisons
        elif isinstance(new_val, (int, float)) and isinstance(old_val, (int, float)):
            try:
                # Convert to strings first to handle precision properly
                new_str = str(new_val)
                old_str = str(old_val)

                # Get decimal precision (digits after decimal point)
                new_precision = len(new_str.split('.')[-1]) if '.' in new_str else 0
                old_precision = len(old_str.split('.')[-1]) if '.' in old_str else 0

                # Use the lower precision for comparison
                min_precision = min(new_precision, old_precision)

                # Truncate to the minimum precision instead of rounding
                factor = 10 ** min_precision
                new_float_trunc = math.trunc(float(new_val) * factor) / factor
                old_float_trunc = math.trunc(float(old_val) * factor) / factor

                new_float_round = round(float(new_val), min_precision)
                old_float_round = round(float(old_val), min_precision)

                return (new_float_trunc != old_float_trunc) and (new_float_round != old_float_round)
            except (ValueError, TypeError):
                return True
        # Direct comparison
        else:
            return new_val != old_val
    # If new value exists but old is null
    elif pd.notna(new_val) and pd.isna(old_val):
        return True
    return False


def apply_updates(cursor, symbol, table_name, row, update_values, merge_keys):
    """
    Apply updates to the database.

    Parameters
    ----------
    cursor
        Database cursor
    symbol : str
        Stock symbol being processed
    table_name : str
        Name of the database table
    row
        The row being updated
    update_values : dict
        Dictionary of column names to new values
    merge_keys : list
        List of columns used as merge keys
    """
    if not update_values:
        return

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


def get_company_tickers():
    """
    Fetch the latest company tickers JSON from SEC website.

    Returns
    -------
    dict
        A dictionary of company tickers and their information

    Notes
    -----
    Falls back to a local file if the SEC website is unavailable
    """
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


def connect_to_database(db_init_file="database_dev_v2.ini", section="postgresql"):
    """
    Establish a connection to the database.

    Parameters
    ----------
    db_init_file : str, default="database_dev_v2.ini"
        Path to the database configuration file
    section : str, default="postgresql"
        Section in the configuration file to use

    Returns
    -------
    connection
        A database connection object
    """
    db_config = load_config(filename=db_init_file, section=section)
    connection = connect(db_config)

    if connection:
        print("Connected successfully!")
        return connection
    else:
        raise ValueError(f"Failed to connect to db: {db_config}")


def handle_rate_limiting(counter, start_time, api_limit_per_min=300):
    """
    Handle API rate limiting by sleeping if necessary.

    Parameters
    ----------
    counter : int
        Current count of API calls
    start_time : float
        Time when counting started
    api_limit_per_min : int, default=300
        Maximum API calls allowed per minute

    Returns
    -------
    tuple
        Updated counter and start_time
    """
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

    return counter, start_time


def process_symbol(connection, symbol, api_key, failure_list=None, period="quarter"):
    """
    Process a single symbol by adding its datasets to the database.

    Parameters
    ----------
    connection
        Database connection
    symbol : str
        Stock symbol to process
    api_key : str
        API key for the financial data provider
    failure_list : list, optional
        List to append failed symbols to
    period : str, default="quarter"
        Data period ("quarter" or "fy")
    """
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
        period=period,
        key=api_key,
        failure_list=failure_list,
    )


def main_quarter(api_key, db_init_file="database_dev_v2.ini", section="postgresql"):
    """
    Main function to process quarterly financial data for all companies.

    Parameters
    ----------
    api_key : str
        API key for the financial data provider
    db_init_file : str, default="database_dev_v2.ini"
        Path to the database configuration file
    section : str, default="postgresql"
        Section in the configuration file to use

    Returns
    -------
    list
        List of symbols that failed processing
    """
    # Initialize list to track failures
    symbols_with_failure = []

    # Connect to the database
    connection = connect_to_database(db_init_file, section)

    # Get company tickers
    ticker_dict = get_company_tickers()

    # Initialize rate limiting variables
    counter = 0
    start_time = time.time()

    # Process each symbol
    for value_dict in ticker_dict.values():
        # Handle API rate limiting
        counter, start_time = handle_rate_limiting(counter, start_time)

        # Process the current symbol
        symbol = value_dict["ticker"]
        process_symbol(connection, symbol, api_key, symbols_with_failure, period="quarter")
        counter += 1

    return symbols_with_failure


def load_api_key(key_file="key.txt"):
    """
    Load the API key from a file.

    Parameters
    ----------
    key_file : str, default="key.txt"
        Path to the file containing the API key

    Returns
    -------
    str
        The API key as a string
    """
    key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), key_file)
    with open(key_path) as f:
        return f.readlines()[0].strip()


if __name__ == "__main__":
    # Load API key
    api_key = load_api_key()

    # Set up database configuration
    db_init_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database_v2.ini")

    # Run the main function
    failed_symbols = main_quarter(api_key, db_init_file=db_init_file, section="postgresql")

    # Print any failures
    print(f"The following symbols failed: {failed_symbols}")
