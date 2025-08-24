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

Command Options
===============

All commands support the following options:

* ``--config, -c``: Path to configuration file (default: ``db/db/config.json``)
* ``--help``: Show help information for any command

For data ingestion commands:

* ``--file, -f``: Path to file containing ticker symbols (one per line)
* ``--period, -p``: Data period to ingest (``quarter``, ``fy``, or ``all``)

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
    ├── project_eden/          # Main package
    │   ├── cli.py            # Command-line interface
    │   ├── db/               # Database modules
    │   │   ├── config.json   # Configuration files
    │   │   ├── create_tables.py
    │   │   ├── data_ingestor.py
    │   │   └── utils.py
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
