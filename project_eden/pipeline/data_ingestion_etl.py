from zenml import pipeline
from typing import List, Optional
from project_eden.db.data_ingestor import Datasets
from project_eden.steps.data_ingestion import (
    load_configuration_step,
    get_tickers_step,
    ingest_all_tickers_step,
)


@pipeline
def financial_data_ingestion_pipeline(
    config_file: str = "config.json",
    tickers: Optional[List[str]] = None,
    datasets: Optional[List[Datasets]] = None,
    period: str = "quarter",
):
    """
    ZenML pipeline for ingesting financial data with rate limiting.

    This pipeline orchestrates the following steps:
    1. Load configuration from JSON file
    2. Get list of tickers to process (either from input or fetch all)
    3. Ingest data for all tickers with automatic rate limiting

    The pipeline respects the API rate limit specified in the configuration file
    (rate_limit_per_min) and will automatically sleep when necessary to avoid
    exceeding the limit.

    Parameters
    ----------
    config_file : str, default="config.json"
        Path to the configuration file
    tickers : Optional[List[str]], default=None
        List of stock ticker symbols to process. If None, processes all companies.
    datasets : Optional[List[Datasets]], default=None
        List of datasets to ingest. If None, ingests all default datasets.
    period : str, default="quarter"
        Period for data ingestion ("quarter" or "fy")

    Returns
    -------
    List[Tuple[str, bool]]
        List of tuples containing (ticker, success_status) for each processed ticker
    """
    # Step 1: Load configuration
    config = load_configuration_step(config_file=config_file)

    # Step 2: Get tickers to process
    tickers_list = get_tickers_step(config=config, tickers=tickers)

    # Step 3: Ingest data for all tickers with rate limiting
    results = ingest_all_tickers_step(
        tickers_list=tickers_list,
        config=config,
        datasets=datasets,
        period=period
    )

    return results




