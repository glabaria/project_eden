"""
Parallel data ingestion pipeline with rate limiting.

This pipeline uses ZenML's dynamic pipeline features to parallelize ticker ingestion
while respecting API rate limits through a shared token bucket rate limiter.
"""
from zenml import pipeline, unmapped
from typing import List, Optional
from project_eden.db.data_ingestor import Datasets
from project_eden.steps.data_ingestion import (
    load_configuration_step,
    get_tickers_step,
    initialize_rate_limiter_step,
    ingest_ticker_data_parallel_step,
)


@pipeline(dynamic=True, enable_cache=False)
def financial_data_ingestion_parallel_pipeline(
    config_file: str = "config.json",
    tickers: Optional[List[str]] = None,
    datasets: Optional[List[Datasets]] = None,
    period: str = "quarter",
):
    """
    ZenML dynamic pipeline for parallel ingestion of financial data with rate limiting.

    This pipeline uses a shared token bucket rate limiter to coordinate parallel workers,
    ensuring that the total API call rate across all workers doesn't exceed the configured
    rate limit.

    The pipeline orchestrates the following steps:
    1. Load configuration from JSON file
    2. Get list of tickers to process (either from input or fetch all)
    3. Initialize the shared rate limiter
    4. Ingest data for all tickers in parallel using .map()

    Each parallel worker acquires tokens from the shared rate limiter before making
    API calls, ensuring compliance with rate limits while maximizing parallelism.

    Parameters
    ----------
    config_file : str, default="config.json"
        Path to the configuration file
    tickers : Optional[List[str]], default=None
        List of stock ticker symbols to process. If None, processes all companies.
    datasets : Optional[List[Datasets]], default=None
        List of datasets to ingest. If None, ingests all default datasets.
    period : str, default="quarter"
        Period for data ingestion ("quarter", "fy", or "all").
        If "all", ingests both quarterly and fiscal year data.

    Returns
    -------
    List[Tuple[str, bool]]
        List of tuples containing (ticker, success_status) for each processed ticker

    Examples
    --------
    Run with specific tickers for quarterly data:
    >>> financial_data_ingestion_parallel_pipeline(
    ...     config_file="config.json",
    ...     tickers=["AAPL", "MSFT", "GOOG"],
    ...     period="quarter"
    ... )

    Run with all tickers for both periods:
    >>> financial_data_ingestion_parallel_pipeline(
    ...     config_file="config.json",
    ...     tickers=None,
    ...     period="all"
    ... )
    """
    # Step 1: Load configuration
    config = load_configuration_step(config_file=config_file)

    # Step 2: Get tickers to process
    tickers_list = get_tickers_step(config=config, tickers=tickers)

    # Step 3: Initialize the shared rate limiter
    initialize_rate_limiter_step(config=config)

    # Step 4: Ingest data for all tickers in parallel using .map()
    # We pass config_file and period as simple strings to avoid serialization issues
    # datasets parameter is not passed - each worker will use default datasets
    results = ingest_ticker_data_parallel_step.map(
        ticker=tickers_list,
        config_file=unmapped(config_file),
        period=unmapped(period)
    )

    return results

