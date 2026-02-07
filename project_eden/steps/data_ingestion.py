from zenml import step
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import time
from project_eden.db.data_ingestor import (
    get_company_tickers,
    gather_dataset,
    load_config,
    Datasets,
    connect_to_database,
    add_datasets_to_db,
    handle_rate_limiting,
)

@step
def load_configuration_step(config_file: str = "config.json") -> Dict[str, Any]:
    """Load configuration from JSON file."""
    return load_config(config_file)

@step
def get_tickers_step(
    config: Dict[str, Any], 
    tickers: Optional[List[str]] = None
) -> List[str]:
    """Get list of tickers to process."""
    if tickers is None:
        ticker_dict = get_company_tickers(config)
        return [value_dict["ticker"] for value_dict in ticker_dict.values()]
    return [ticker.upper() for ticker in tickers]

@step
def fetch_financial_data_step(
    ticker: str,
    dataset: str,
    config: Dict[str, Any],
    period: str = "quarter"
) -> pd.DataFrame:
    """Fetch financial data for a single ticker and dataset."""
    return gather_dataset(
        ticker=ticker,
        dataset=dataset,
        config=config,
        period=period
    )

@step
def rate_limit_step(
    counter: int,
    start_time: float,
    config: Dict[str, Any],
    api_calls_count: int = 5
) -> Tuple[int, float]:
    """
    Handle API rate limiting between ticker processing.

    Parameters
    ----------
    counter : int
        Current count of API calls made
    start_time : float
        Time when the current rate limit window started
    config : Dict[str, Any]
        Configuration dictionary containing rate limit settings
    api_calls_count : int, default=5
        Number of API calls made for the last ticker (default is 5 datasets)

    Returns
    -------
    Tuple[int, float]
        Updated counter and start_time after rate limiting
    """
    # Increment counter by the number of API calls made
    counter += api_calls_count

    # Handle rate limiting
    counter, start_time = handle_rate_limiting(counter, start_time, config)

    return counter, start_time


@step
def ingest_ticker_data_step(
    ticker: str,
    config: Dict[str, Any],
    datasets: Optional[List[Datasets]] = None,
    period: str = "quarter"
) -> Tuple[str, bool]:
    """Ingest all datasets for a single ticker."""
    try:
        connection = connect_to_database(config)

        datasets_to_process = datasets or [
            Datasets.PROFILE,
            Datasets.INCOME_STATEMENT,
            Datasets.CASH_FLOW_STATEMENT,
            Datasets.BALANCE_SHEET_STATEMENT,
            Datasets.HISTORTICAL_PRICE_EOD_FULL,
        ]

        add_datasets_to_db(
            connection=connection,
            symbol=ticker,
            datasets=datasets_to_process,
            config=config,
            period=period
        )

        connection.close()
        return ticker, True

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return ticker, False


@step
def ingest_all_tickers_step(
    tickers_list: List[str],
    config: Dict[str, Any],
    datasets: Optional[List[Datasets]] = None,
    period: str = "quarter"
) -> List[Tuple[str, bool]]:
    """
    Ingest data for all tickers with rate limiting.

    This step processes each ticker sequentially with proper rate limiting
    to respect API constraints.

    Parameters
    ----------
    tickers_list : List[str]
        List of ticker symbols to process
    config : Dict[str, Any]
        Configuration dictionary containing API and rate limit settings
    datasets : Optional[List[Datasets]], default=None
        List of datasets to ingest. If None, ingests all default datasets.
    period : str, default="quarter"
        Period for data ingestion ("quarter" or "fy")

    Returns
    -------
    List[Tuple[str, bool]]
        List of tuples containing (ticker, success_status) for each processed ticker
    """
    # Initialize rate limiting variables
    counter = 0
    start_time = time.time()

    # Determine number of API calls per ticker based on datasets
    datasets_to_process = datasets or [
        Datasets.PROFILE,
        Datasets.INCOME_STATEMENT,
        Datasets.CASH_FLOW_STATEMENT,
        Datasets.BALANCE_SHEET_STATEMENT,
        Datasets.HISTORTICAL_PRICE_EOD_FULL,
    ]
    api_calls_per_ticker = len(datasets_to_process)

    results = []

    for ticker in tickers_list:
        # Apply rate limiting before processing each ticker
        counter += api_calls_per_ticker
        counter, start_time = handle_rate_limiting(counter, start_time, config)

        # Ingest data for the ticker
        try:
            connection = connect_to_database(config)

            add_datasets_to_db(
                connection=connection,
                symbol=ticker,
                datasets=datasets_to_process,
                config=config,
                period=period
            )

            connection.close()
            results.append((ticker, True))
            print(f"Successfully processed {ticker}")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            results.append((ticker, False))

    return results