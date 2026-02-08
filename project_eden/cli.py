#!/usr/bin/env python
"""
Eden CLI - Command line interface for Project Eden
"""
import click
from typing import List
import os
import sys
from click.formatting import HelpFormatter

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
db_path = os.path.join(project_root, 'db')
sys.path.insert(0, db_path)

# Adjust default config path to be absolute
DEFAULT_CONFIG_PATH = os.path.join(project_root, "db/db/config.json")

import project_eden.db.data_ingestor as data_ingestor
import project_eden.db.create_tables as create_tables
from project_eden.pipeline import (
    financial_data_ingestion_pipeline,
    financial_data_ingestion_parallel_pipeline,
)


@click.group()
def cli():
    """Eden CLI - A tool for financial data ingestion and analysis."""
    pass


# Override the main CLI help to ensure proper formatting
original_cli_help = cli.get_help

def custom_cli_help(ctx):
    help_text = original_cli_help(ctx)

    # Define our custom commands section
    commands_section = """Commands:
  init      Initialize database tables and ingest financial data
  ingest    Ingest financial data for specified company tickers
  create    Create database tables for financial data

Run 'eden COMMAND --help' for more information on a command.
"""

    # Replace the default Commands section with our custom one
    if "Commands:" in help_text:
        parts = help_text.split("Commands:")
        # Keep the part before "Commands:" and replace everything between "Commands:" and "Options:"
        if "Options:" in parts[1]:
            options_part = parts[1].split("Options:")[1]
            return parts[0] + commands_section + "Options:" + options_part
        else:
            return parts[0] + commands_section
    else:
        # If no Commands section exists, just add ours before Options
        if "Options:" in help_text:
            parts = help_text.split("Options:")
            return parts[0] + commands_section + "Options:" + parts[1]
        else:
            return help_text + commands_section

cli.get_help = custom_cli_help


@cli.command(help="Ingest financial data for specified company tickers.  "
                  "\n\nTICKERS: One or more stock ticker symbols (e.g., AAPL MSFT GOOG). If not provided, all "
                  "publicly traded companies registered to the SEC (https://www.sec.gov/files/company_tickers.json) "
                  "will be processed. Alternatively, provide a file with ticker symbols using the --file option.")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=DEFAULT_CONFIG_PATH,
    help="Path to the configuration file",
)
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True),
    help="Path to a file containing ticker symbols (one per line)",
)
@click.option(
    "--period",
    "-p",
    type=click.Choice(["quarter", "fy", "all"], case_sensitive=False),
    default=None,
    help="Data period to ingest (quarter, fy, or both if not specified or all)",
)
@click.option(
    "--pipeline",
    is_flag=True,
    default=False,
    help="Use ZenML pipeline for execution (enables tracking, observability, and reproducibility)",
)
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Use parallel execution with rate limiting (requires --pipeline flag)",
)
@click.argument("tickers", nargs=-1, required=False)
def ingest(config: str, file: str = None, period: str = None, pipeline: bool = False, parallel: bool = False, tickers: List[str] = None):
    """
    Ingest financial data for specified company tickers.   Type `eden ingest --help` for more information.

    TICKERS: One or more stock ticker symbols (e.g., AAPL MSFT GOOG).
            If not provided, all publicly traded companies registered to the SEC
            (https://www.sec.gov/files/company_tickers.json) will be processed.

    Alternatively, provide a file with ticker symbols using the --file option.
    """
    if file:
        with open(file, 'r') as f:
            file_tickers = [line.strip() for line in f if line.strip()]
        if tickers:
            tickers = list(tickers) + file_tickers
        else:
            tickers = file_tickers

    tickers = None if not tickers else tickers

    # Handle period parameter
    if pipeline:
        # Pipeline mode: Convert None to "all" to avoid ZenML serialization issues
        # ZenML has trouble with unmapped(None) values
        period_value = "all" if period is None or period == "all" else period
    else:
        # Non-pipeline mode: Use None to mean "both periods"
        period_value = None if period == "all" or period is None else period

    if pipeline:
        # Validate parallel flag
        if parallel and not pipeline:
            print("Error: --parallel flag requires --pipeline flag")
            return

        # Use ZenML pipeline for execution
        if parallel:
            print("Running ingestion using ZenML parallel pipeline with rate limiting...")
            pipeline_run = financial_data_ingestion_parallel_pipeline(
                config_file=config,
                tickers=tickers,
                period=period_value
            )
            # Extract results from parallel pipeline (.map() creates multiple step instances)
            # Each mapped invocation creates a separate step (ingest_ticker_data_parallel_step,
            # ingest_ticker_data_parallel_step_2, etc.)
            results = []
            for step_name in pipeline_run.steps.keys():
                if "ingest_ticker_data_parallel_step" in step_name:
                    step_outputs = pipeline_run.steps[step_name].outputs
                    # Each step returns Tuple[str, bool], so it has output_0 (ticker) and output_1 (success)
                    ticker = step_outputs['output_0'][0].load()
                    success = step_outputs['output_1'][0].load()
                    results.append((ticker, success))
        else:
            print("Running ingestion using ZenML pipeline (sequential)...")
            results = financial_data_ingestion_pipeline(
                config_file=config,
                tickers=tickers,
                period=period_value
            )

        # Report results
        failed = [ticker for ticker, success in results if not success]
        successful = [ticker for ticker, success in results if success]

        print(f"\nIngestion complete!")
        print(f"Successful: {len(successful)} tickers")
        if failed:
            print(f"Failed: {len(failed)} tickers - {failed}")
    else:
        # Use direct execution (original behavior)
        data_ingestor.driver(config_file=config, tickers=tickers, period=period_value)


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=DEFAULT_CONFIG_PATH,
    help="Path to the configuration file",
)
@click.argument("tables", nargs=-1, required=False)
def create(config: str, tables: List[str] = None):
    """Create database tables for financial data.  Type `eden create --help` for more information."""
    create_tables.driver(config_file=config, tables=tables)


# Override the get_help method to provide custom formatted help
original_format_help = create.get_help

def custom_format_help(ctx):
    formatter = HelpFormatter()
    formatter.write_heading("Create database tables for financial data")
    formatter.write_paragraph()
    formatter.write_text("TABLES: One or more table names to create. If not provided, all tables will be created.")
    formatter.write_paragraph()
    formatter.write_text("Available tables:")
    for table in create_tables.AvailableTables:
        formatter.write_text(f"  • {table.value}")

    # Get the original help text for options and arguments
    original_help = original_format_help(ctx)

    # Extract just the options part (after "Options:")
    options_part = original_help.split("Options:")[1] if "Options:" in original_help else ""

    return formatter.getvalue() + "\nOptions:" + options_part

create.get_help = custom_format_help


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=DEFAULT_CONFIG_PATH,
    help="Path to the configuration file",
)
@click.option(
    "--period",
    "-p",
    type=click.Choice(["quarter", "fy", "all"], case_sensitive=False),
    default=None,
    help="Data period to ingest (quarter, fy, or both if not specified or all)",
)
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True),
    help="Path to a file containing ticker symbols (one per line)",
)
@click.option(
    "--pipeline",
    is_flag=True,
    default=False,
    help="Use ZenML pipeline for execution (enables tracking, observability, and reproducibility)",
)
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Use parallel execution with rate limiting (requires --pipeline flag)",
)
@click.argument("tickers", nargs=-1, required=False)
def init(config: str, file: str = None, period: str = None, pipeline: bool = False, parallel: bool = False, tickers: List[str] = None):
    """
    Initialize database tables and ingest financial data.

    Creates all necessary database tables and ingests financial data for specified tickers.
    If no tickers are specified, processes all publicly traded companies.
    """
    # Create all tables
    create_tables.driver(config_file=config, tables=None)

    # Process tickers from file if provided
    if file:
        with open(file, 'r') as f:
            file_tickers = [line.strip() for line in f if line.strip()]
        if tickers:
            tickers = list(tickers) + file_tickers
        else:
            tickers = file_tickers

    # Ingest data
    tickers_list = None if not tickers else tickers

    # Handle period parameter
    if pipeline:
        # Pipeline mode: Convert None to "all" to avoid ZenML serialization issues
        # ZenML has trouble with unmapped(None) values
        period_value = "all" if period is None or period == "all" else period
    else:
        # Non-pipeline mode: Use None to mean "both periods"
        period_value = None if period == "all" or period is None else period

    if pipeline:
        # Validate parallel flag
        if parallel and not pipeline:
            print("Error: --parallel flag requires --pipeline flag")
            return

        # Use ZenML pipeline for execution
        if parallel:
            print("Running ingestion using ZenML parallel pipeline with rate limiting...")
            pipeline_run = financial_data_ingestion_parallel_pipeline(
                config_file=config,
                tickers=tickers_list,
                period=period_value
            )
            # Extract results from parallel pipeline (.map() creates multiple step instances)
            # Each mapped invocation creates a separate step (ingest_ticker_data_parallel_step,
            # ingest_ticker_data_parallel_step_2, etc.)
            results = []
            for step_name in pipeline_run.steps.keys():
                if "ingest_ticker_data_parallel_step" in step_name:
                    step_outputs = pipeline_run.steps[step_name].outputs
                    # Each step returns Tuple[str, bool], so it has output_0 (ticker) and output_1 (success)
                    ticker = step_outputs['output_0'][0].load()
                    success = step_outputs['output_1'][0].load()
                    results.append((ticker, success))
        else:
            print("Running ingestion using ZenML pipeline (sequential)...")
            results = financial_data_ingestion_pipeline(
                config_file=config,
                tickers=tickers_list,
                period=period_value
            )

        # Report results
        failed = [ticker for ticker, success in results if not success]
        successful = [ticker for ticker, success in results if success]

        print(f"\nIngestion complete!")
        print(f"Successful: {len(successful)} tickers")
        if failed:
            print(f"Failed: {len(failed)} tickers - {failed}")
    else:
        # Use direct execution (original behavior)
        data_ingestor.driver(config_file=config, tickers=tickers_list, period=period_value)


# Override the get_help method to provide custom formatted help
original_init_help = init.get_help


def custom_init_help(ctx):
    formatter = HelpFormatter()
    formatter.write_heading("Initialize database tables and ingest financial data")
    formatter.write_paragraph()
    formatter.write_text("Creates all necessary database tables and ingests financial data for specified tickers.")
    formatter.write_paragraph()
    formatter.write_text("TICKERS: One or more stock ticker symbols (e.g., AAPL MSFT GOOG).")
    formatter.write_text("         If not provided, all publicly traded companies will be processed.")

    # Get the original help text for options and arguments
    original_help = original_init_help(ctx)

    # Extract just the options part (after "Options:")
    options_part = original_help.split("Options:")[1] if "Options:" in original_help else ""

    return formatter.getvalue() + "\nOptions:" + options_part


init.get_help = custom_init_help


if __name__ == "__main__":
    cli()