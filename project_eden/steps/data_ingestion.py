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
from project_eden.utils.rate_limiter import get_rate_limiter

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
        Period for data ingestion ("quarter", "fy", or "all").
        If "all", ingests both quarterly and fiscal year data.

    Returns
    -------
    List[Tuple[str, bool]]
        List of tuples containing (ticker, success_status) for each processed ticker
    """
    # Initialize rate limiting variables
    counter = 0
    start_time = time.time()

    # Determine if we need to process both periods
    process_both_periods = period is None or period == "all"

    if process_both_periods:
        # For both periods: quarterly gets all datasets, fiscal year excludes PROFILE
        datasets_quarter = datasets or [
            Datasets.PROFILE,
            Datasets.INCOME_STATEMENT,
            Datasets.CASH_FLOW_STATEMENT,
            Datasets.BALANCE_SHEET_STATEMENT,
            Datasets.HISTORTICAL_PRICE_EOD_FULL,
        ]
        datasets_fy = datasets or [
            Datasets.INCOME_STATEMENT,
            Datasets.CASH_FLOW_STATEMENT,
            Datasets.BALANCE_SHEET_STATEMENT,
        ]
        api_calls_per_ticker = len(datasets_quarter) + len(datasets_fy)
    else:
        # Single period processing
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

            if process_both_periods:
                # Process quarterly data
                add_datasets_to_db(
                    connection=connection,
                    symbol=ticker,
                    datasets=datasets_quarter,
                    config=config,
                    period="quarter"
                )

                # Process fiscal year data
                add_datasets_to_db(
                    connection=connection,
                    symbol=ticker,
                    datasets=datasets_fy,
                    config=config,
                    period="fy"
                )
                print(f"Successfully processed {ticker} (both periods)")
            else:
                # Process single period
                add_datasets_to_db(
                    connection=connection,
                    symbol=ticker,
                    datasets=datasets_to_process,
                    config=config,
                    period=period
                )
                print(f"Successfully processed {ticker}")

            connection.close()
            results.append((ticker, True))

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            results.append((ticker, False))

    return results


@step
def initialize_rate_limiter_step(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Initialize the global rate limiter for parallel execution.

    This step should be called once at the beginning of a parallel pipeline
    to set up the shared rate limiter that all parallel workers will use.

    Parameters
    ----------
    config : Dict[str, Any]
        Configuration dictionary containing rate_limit_per_min

    Returns
    -------
    Dict[str, Any]
        The same config (for chaining)
    """
    rate_limiter = get_rate_limiter(config)
    print(f"Rate limiter initialized: {config['api']['rate_limit_per_min']} calls/min")
    print(f"Available tokens: {rate_limiter.get_available_tokens():.2f}")
    return config


@step
def ingest_ticker_data_parallel_step(
    ticker: str,
    config_file: str,
    datasets: Optional[List[Datasets]] = None,
    period: str = "quarter"
) -> Tuple[str, bool]:
    """
    Ingest all datasets for a single ticker with rate limiting for parallel execution.

    This step uses a shared rate limiter to coordinate with other parallel workers,
    ensuring the total API call rate across all workers doesn't exceed the limit.

    Parameters
    ----------
    ticker : str
        Stock ticker symbol to process
    config_file : str
        Path to configuration file
    datasets : Optional[List[Datasets]], default=None
        List of datasets to ingest. If None, ingests all default datasets.
    period : str, default="quarter"
        Period for data ingestion ("quarter", "fy", or "all").
        If "all", ingests both quarterly and fiscal year data.

    Returns
    -------
    Tuple[str, bool]
        Tuple of (ticker, success_status)
    """
    try:
        # Load configuration
        config = load_config(config_file)

        # Determine if we need to process both periods
        process_both_periods = period is None or period == "all"

        if process_both_periods:
            # Process quarterly data first with all datasets
            datasets_quarter = datasets or [
                Datasets.PROFILE,
                Datasets.INCOME_STATEMENT,
                Datasets.CASH_FLOW_STATEMENT,
                Datasets.BALANCE_SHEET_STATEMENT,
                Datasets.HISTORTICAL_PRICE_EOD_FULL,
            ]

            # Process fiscal year data with datasets excluding PROFILE (not period-specific)
            datasets_fy = datasets or [
                Datasets.INCOME_STATEMENT,
                Datasets.CASH_FLOW_STATEMENT,
                Datasets.BALANCE_SHEET_STATEMENT,
            ]

            num_api_calls = len(datasets_quarter) + len(datasets_fy)

            # Acquire tokens from the shared rate limiter before making API calls
            rate_limiter = get_rate_limiter(config)
            print(f"[{ticker}] Acquiring {num_api_calls} tokens from rate limiter (both periods)...")
            rate_limiter.acquire(num_api_calls)
            print(f"[{ticker}] Tokens acquired, starting ingestion for both periods...")

            # Now we have permission to make the API calls
            connection = connect_to_database(config)

            # Process quarterly data
            add_datasets_to_db(
                connection=connection,
                symbol=ticker,
                datasets=datasets_quarter,
                config=config,
                period="quarter"
            )

            # Process fiscal year data
            add_datasets_to_db(
                connection=connection,
                symbol=ticker,
                datasets=datasets_fy,
                config=config,
                period="fy"
            )

            connection.close()
            print(f"[{ticker}] Successfully processed both periods")
            return ticker, True
        else:
            # Process single period
            datasets_to_process = datasets or [
                Datasets.PROFILE,
                Datasets.INCOME_STATEMENT,
                Datasets.CASH_FLOW_STATEMENT,
                Datasets.BALANCE_SHEET_STATEMENT,
                Datasets.HISTORTICAL_PRICE_EOD_FULL,
            ]
            num_api_calls = len(datasets_to_process)

            # Acquire tokens from the shared rate limiter before making API calls
            rate_limiter = get_rate_limiter(config)
            print(f"[{ticker}] Acquiring {num_api_calls} tokens from rate limiter...")
            rate_limiter.acquire(num_api_calls)
            print(f"[{ticker}] Tokens acquired, starting ingestion...")

            # Now we have permission to make the API calls
            connection = connect_to_database(config)

            add_datasets_to_db(
                connection=connection,
                symbol=ticker,
                datasets=datasets_to_process,
                config=config,
                period=period
            )

            connection.close()
            print(f"[{ticker}] Successfully processed")
            return ticker, True

    except Exception as e:
        print(f"[{ticker}] Error processing: {e}")
        return ticker, False