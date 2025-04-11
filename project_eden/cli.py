#!/usr/bin/env python
"""
Eden CLI - Command line interface for Project Eden
"""
import click
from typing import List
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
db_path = os.path.join(project_root, 'db')
sys.path.insert(0, db_path)

# Adjust default config path to be absolute
DEFAULT_CONFIG_PATH = os.path.join(project_root, "db/db/config.json")

import db.data_ingestor as data_ingestor


@click.group()
def cli():
    """Eden CLI - A tool for financial data ingestion and analysis."""
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=DEFAULT_CONFIG_PATH,
    help="Path to the configuration file",
)
@click.argument("tickers", nargs=-1, required=False)
def ingest(config: str, tickers: List[str] = None):
    """
    Ingest financial data for specified company tickers.

    TICKERS: One or more stock ticker symbols (e.g., AAPL MSFT GOOG).
            If not provided, all publicly traded companies registered to the SEC
            (https://www.sec.gov/files/company_tickers.json) will be processed.
    """
    tickers = None if not tickers else tickers
    data_ingestor.driver(config_file=config, tickers=tickers)


if __name__ == "__main__":
    cli()
