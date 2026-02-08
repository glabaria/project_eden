============
Project Eden
============

.. image:: ./assets/repo_logo.png
   :alt: Project Eden Logo
   :align: center

Project Eden is a fundamental analysis engine that empowers investors to make smarter investment decisions by ingesting and analyzing financial data from publicly traded companies.

Features
========

* **Financial Data Ingestion**: Automatically fetch financial data for any publicly traded company
* **Database Management**: Create and manage database tables for financial data storage
* **CLI Interface**: Easy-to-use command-line interface for all operations
* **Flexible Data Periods**: Support for quarterly and fiscal year data
* **Batch Processing**: Process multiple companies at once or all SEC-registered companies

Requirements
============

* Python 3.12+
* Poetry (for dependency management)

Installation
============

1. **Clone the repository**::

    git clone <repository-url>
    cd project_eden

2. **Install Poetry** (if not already installed)::

    # On Windows
    powershell -ExecutionPolicy ByPass -c "irm https://install.python-poetry.org | iex"

    # On macOS/Linux
    curl -sSL https://install.python-poetry.org | python3 -

3. **Install dependencies**::

    poetry install

4. **Activate the virtual environment**::

    poetry shell

Usage
=====

Project Eden provides a command-line interface through the ``eden`` command.

Initialize Database and Ingest Data
-----------------------------------

To set up the database and start ingesting financial data::

    # Initialize with all publicly traded companies
    eden init

    # Initialize with specific tickers
    eden init AAPL MSFT GOOG

    # Initialize with tickers from a file
    eden init --file tickers.txt

    # Initialize with specific data period
    eden init --period quarter AAPL MSFT

Create Database Tables
----------------------

To create database tables without ingesting data::

    # Create all tables
    eden create

    # Create specific tables
    eden create table1 table2

Ingest Financial Data
---------------------

To ingest financial data for specific companies::

    # Ingest data for specific tickers
    eden ingest AAPL MSFT GOOG

    # Ingest data for all publicly traded companies
    eden ingest

    # Ingest data from a file of tickers
    eden ingest --file portfolio_tickers.txt

    # Ingest only quarterly data
    eden ingest --period quarter AAPL

    # Ingest only fiscal year data
    eden ingest --period fy AAPL

Using ZenML Pipeline Mode
--------------------------

For enhanced tracking, observability, and reproducibility, you can use the ``--pipeline`` flag to execute ingestion through ZenML pipelines::

    # Use ZenML pipeline for ingestion
    eden ingest --pipeline AAPL MSFT GOOG

    # Initialize with pipeline mode
    eden init --pipeline AAPL MSFT

**Benefits of Pipeline Mode:**

* **Tracking & Observability**: All pipeline runs are tracked with metadata, parameters, and results
* **Reproducibility**: Every run is versioned with exact parameters for easy re-execution
* **Error Handling**: Better failure isolation and built-in retry mechanisms
* **Lineage Tracking**: Complete audit trail of what data was ingested, when, and with what configuration
* **Scalability**: Easy to switch from local execution to cloud orchestrators (e.g., Kubernetes) without code changes

**When to Use Pipeline Mode:**

* Production data ingestion workflows
* Large-scale ingestion (100+ tickers)
* Scheduled or automated runs
* When you need compliance and audit trails
* When you want to track ingestion history and performance

**When to Use Direct Mode (default):**

* Quick testing or debugging (1-5 tickers)
* Development and experimentation
* Simple one-off ingestions

Using Parallel Pipeline Mode
-----------------------------

For maximum performance when ingesting many tickers, use the ``--parallel`` flag with ``--pipeline`` to enable parallel execution with automatic rate limiting::

    # Parallel ingestion with rate limiting
    eden ingest --pipeline --parallel AAPL MSFT GOOG AMZN META

    # Initialize with parallel mode
    eden init --pipeline --parallel AAPL MSFT

    # Ingest all tickers in parallel
    eden ingest --pipeline --parallel

**How Parallel Mode Works:**

The parallel pipeline uses a **token bucket rate limiter** that coordinates across all parallel workers to ensure the total API call rate never exceeds your configured limit (``rate_limit_per_min`` in config.json).

* Workers process tickers simultaneously
* Each worker acquires "tokens" before making API calls
* Tokens refill at the configured rate (e.g., 300 per minute)
* Workers automatically wait if insufficient tokens are available

**Performance Benefits:**

* **Sequential mode**: Processes ~1 ticker per minute (with 5 datasets)
* **Parallel mode**: Processes up to 60 tickers per minute (with 300 calls/min limit)
* **Example**: 100 tickers takes ~100 minutes sequential vs. ~2-3 minutes parallel

**When to Use Parallel Mode:**

* Large-scale ingestion (50+ tickers)
* When you want to maximize API quota usage
* Production workflows with time constraints
* Batch processing of many tickers

**When to Use Sequential Mode:**

* Small number of tickers (< 10)
* Testing and debugging
* When you want simpler execution flow

See ``docs/PARALLEL_PIPELINE.md`` for detailed documentation on parallel execution.

Command Options
===============

All commands support the following options:

* ``--config, -c``: Path to configuration file (default: ``db/db/config.json``)
* ``--help``: Show help information for any command

For data ingestion commands (``init`` and ``ingest``):

* ``--file, -f``: Path to file containing ticker symbols (one per line)
* ``--period, -p``: Data period to ingest (``quarter``, ``fy``, or ``all``)
* ``--pipeline``: Use ZenML pipeline for execution (enables tracking, observability, and reproducibility)
* ``--parallel``: Use parallel execution with rate limiting (requires ``--pipeline`` flag)

Configuration
=============

The application uses a JSON configuration file located at ``project_eden/db/db/config.json``. You can specify a custom configuration file using the ``--config`` option.

Example configuration files are available in the ``project_eden/db/`` directory:

* ``config_example.json`` - Template configuration file
* ``config_dev.json`` - Development configuration
* ``config_scratch.json`` - Scratch/testing configuration

Development
===========

This project uses Poetry for dependency management and includes development tools:

* **Black**: Code formatting (line length: 99 characters)
* **Python 3.12**: Target Python version

To contribute:

1. Install development dependencies::

    poetry install --with dev

2. Format code with Black::

    poetry run black .

3. Run the CLI in development mode::

    poetry run python -m project_eden.cli

Project Structure
=================

::

    project_eden/
    ├── assets/                 # Project assets (logos, images)
    ├── examples/              # Example scripts
    │   └── run_ingestion_pipeline.py
    ├── project_eden/          # Main package
    │   ├── cli.py            # Command-line interface
    │   ├── db/               # Database modules
    │   │   ├── config.json   # Configuration files
    │   │   ├── create_tables.py
    │   │   ├── data_ingestor.py
    │   │   └── utils.py
    │   ├── pipeline/         # ZenML pipelines
    │   │   └── data_ingestion_etl.py
    │   ├── steps/            # ZenML pipeline steps
    │   │   └── data_ingestion.py
    │   └── __init__.py
    ├── scripts/              # Utility scripts
    ├── pyproject.toml        # Project configuration
    └── README.rst           # This file


Author
======

- George Labaria

License
=======

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
For those who do not wish to adhere to the AGPL's open-source requirements, a separate
commercial license is available. Please contact the author to discuss licensing options. See the AGPL-3.0 text here:
https://www.gnu.org/licenses/agpl-3.0.en.html

Getting Help
============

For more information on any command, use the ``--help`` option::

    eden --help
    eden init --help
    eden ingest --help
    eden create --help
