import psycopg2
import datetime
import os
import json
from psycopg2 import sql
from enum import Enum

from typing import Optional, Tuple, Dict, Any, List
from db.utils import connect


DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "symbol": "text",
    "companyName": "text",
    "currency": "text",
    "cik": "bigint",
    "isin": "text",
    "cusip": "bigint",
    "exchange": "text",
    "exchangeShortName": "text",
    "industry": "text",
    "website": "text",
    "description": "text",
    "ceo": "text",
    "sector": "text",
    "country": "text",
    "fullTimeEmployees": "int",
    "phone": "text",
    "address": "text",
    "city": "text",
    "state": "text",
    "zip": "text",
    "image": "text",
    "ipoDate": "date",
    "isEtf": "bool",
    "isActivelyTrading": "bool",
    "isAdr": "bool",
    "isFund": "bool",
    "price": "real",
    "mktCap": "bigint",
    "beta": "real",
    "lastDiv": "real",
}


DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "company_id": "serial",
    "date": "date",
    "symbol": "text",
    "reportedCurrency": "text",
    "cik": "int",
    "fillingDate": "date",
    "acceptedDate": "timestamp",
    "calendarYear": "smallint",
    "period": "text",
    "revenue": "bigint",
    "costOfRevenue": "bigint",
    "grossProfit": "bigint",
    "grossProfitRatio": "real",
    "researchAndDevelopmentExpenses": "bigint",
    "generalAndAdministrativeExpenses": "bigint",
    "sellingAndMarketingExpenses": "bigint",
    "sellingGeneralAndAdministrativeExpenses": "bigint",
    "otherExpenses": "bigint",
    "operatingExpenses": "bigint",
    "costAndExpenses": "bigint",
    "interestIncome": "bigint",
    "interestExpense": "bigint",
    "depreciationAndAmortization": "bigint",
    "ebitda": "bigint",
    "ebitdaratio": "real",
    "operatingIncome": "bigint",
    "operatingIncomeRatio": "real",
    "totalOtherIncomeExpensesNet": "bigint",
    "incomeBeforeTax": "bigint",
    "incomeBeforeTaxRatio": "real",
    "incomeTaxExpense": "bigint",
    "netIncome": "bigint",
    "netIncomeRatio": "real",
    "eps": "real",
    "epsdiluted": "real",
    "weightedAverageShsOut": "bigint",
    "weightedAverageShsOutDil": "bigint",
    "link": "text",
    "finalLink": "text",
}

DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "company_id": "serial",
    "date": "date",
    "symbol": "text",
    "reportedCurrency": "text",
    "cik": "int",
    "fillingDate": "date",
    "acceptedDate": "timestamp",
    "calendarYear": "smallint",
    "period": "text",
    "cashAndCashEquivalents": "bigint",
    "shortTermInvestments": "bigint",
    "cashAndShortTermInvestments": "bigint",
    "netReceivables": "bigint",
    "inventory": "bigint",
    "otherCurrentAssets": "bigint",
    "totalCurrentAssets": "bigint",
    "propertyPlantEquipmentNet": "bigint",
    "goodwill": "bigint",
    "intangibleAssets": "bigint",
    "goodwillAndIntangibleAssets": "bigint",
    "longTermInvestments": "bigint",
    "taxAssets": "bigint",
    "otherNonCurrentAssets": "bigint",
    "totalNonCurrentAssets": "bigint",
    "otherAssets": "bigint",
    "totalAssets": "bigint",
    "accountPayables": "bigint",
    "shortTermDebt": "bigint",
    "taxPayables": "bigint",
    "deferredRevenue": "bigint",
    "otherCurrentLiabilities": "bigint",
    "totalCurrentLiabilities": "bigint",
    "longTermDebt": "bigint",
    "deferredRevenueNonCurrent": "bigint",
    "deferredTaxLiabilitiesNonCurrent": "bigint",
    "otherNonCurrentLiabilities": "bigint",
    "totalNonCurrentLiabilities": "bigint",
    "otherLiabilities": "bigint",
    "capitalLeaseObligations": "bigint",
    "totalLiabilities": "bigint",
    "preferredStock": "bigint",
    "commonStock": "bigint",
    "retainedEarnings": "bigint",
    "accumulatedOtherComprehensiveIncomeLoss": "bigint",
    "othertotalStockholdersEquity": "bigint",
    "totalStockholdersEquity": "bigint",
    "totalEquity": "bigint",
    "totalLiabilitiesAndStockholdersEquity": "bigint",
    "minorityInterest": "bigint",
    "totalLiabilitiesAndTotalEquity": "bigint",
    "totalInvestments": "bigint",
    "totalDebt": "bigint",
    "netDebt": "bigint",
    "link": "text",
    "finalLink": "text",
}


DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "company_id": "serial",
    "date": "date",
    "symbol": "text",
    "reportedCurrency": "text",
    "cik": "int",
    "fillingDate": "date",
    "acceptedDate": "timestamp",
    "calendarYear": "smallint",
    "period": "text",
    "netIncome": "bigint",
    "depreciationAndAmortization": "bigint",
    "deferredIncomeTax": "bigint",
    "stockBasedCompensation": "bigint",
    "changeInWorkingCapital": "bigint",
    "accountsReceivables": "bigint",
    "inventory": "bigint",
    "accountsPayables": "bigint",
    "otherWorkingCapital": "bigint",
    "otherNonCashItems": "bigint",
    "netCashProvidedByOperatingActivities": "bigint",
    "investmentsInPropertyPlantAndEquipment": "bigint",
    "acquisitionsNet": "bigint",
    "purchasesOfInvestments": "bigint",
    "salesMaturitiesOfInvestments": "bigint",
    "otherInvestingActivites": "bigint",
    "netCashUsedForInvestingActivites": "bigint",
    "debtRepayment": "bigint",
    "commonStockIssued": "bigint",
    "commonStockRepurchased": "bigint",
    "dividendsPaid": "bigint",
    "otherFinancingActivites": "bigint",
    "netCashUsedProvidedByFinancingActivities": "bigint",
    "effectOfForexChangesOnCash": "bigint",
    "netChangeInCash": "bigint",
    "cashAtEndOfPeriod": "bigint",
    "cashAtBeginningOfPeriod": "bigint",
    "operatingCashFlow": "bigint",
    "capitalExpenditure": "bigint",
    "freeCashFlow": "bigint",
    "link": "text",
    "finalLink": "text",
}

DEFAULT_CURRENT_PRICE_COLUMNS_TO_TYPE = {
    "company_id": "serial",
    "date": "date",
    "marketCap": "bigint",
}

DEFAULT_SHARES_COLUMNS_TO_TYPE = {
    "company_id": "serial",
    "date": "date",
    "numberofshares": "bigint",
}

DEFAULT_PRICE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "company_id": "serial",
    "symbol": "text",
    "date": "date",
    "open": "real",
    "high": "real",
    "low": "real",
    "close": "real",
    "volume": "bigint",
}

FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES = {
    x: x.lower() for x in DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE.keys()
}
FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.update(
    {x: x.lower() for x in DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()}
)
FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.update(
    {x: x.lower() for x in DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.keys()}
)
FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.update(
    {x: x.lower() for x in DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.keys()}
)
FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.update(
    {x: x.lower() for x in DEFAULT_PRICE_COLUMNS_TO_TYPE.keys()}
)

POSTGRES_COLUMN_NAMES_TO_FMP_COLUMN_NAMES = {
    x: y for y, x in FMP_COLUMN_NAMES_TO_POSTGRES_COLUMN_NAMES.items()
}

POSTGRES_TYPE_TO_PYTHON_TYPE = {
    "serial": int,
    "int": int,
    "bigint": int,
    "smallint": int,
    "text": str,
    "date": datetime.date,
    "timestamp": datetime.date,
    "real": float,
    "bool": bool,
}


class AvailableTables(Enum):
    """Enum of tables that can be created by the driver."""
    COMPANY = "company"
    INCOME_STATEMENT_FY = "income_statement_fy"
    INCOME_STATEMENT_QUARTER = "income_statement_quarter"
    BALANCE_SHEET_FY = "balance_sheet_fy"
    BALANCE_SHEET_QUARTER = "balance_sheet_quarter"
    CASH_FLOW_STATEMENT_FY = "cash_flow_statement_fy"
    CASH_FLOW_STATEMENT_QUARTER = "cash_flow_statement_quarter"
    SHARES_FY = "shares_fy"
    PRICE = "price"


def postgres_type_to_python_type(column_name: str, is_postgres_column_name: bool = True) -> type:
    column_name = (
        column_name
        if not is_postgres_column_name
        else POSTGRES_COLUMN_NAMES_TO_FMP_COLUMN_NAMES[column_name]
    )
    x = (
        DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE.get(column_name)
        or DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.get(column_name)
        or DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.get(column_name)
        or DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.get(column_name)
        or DEFAULT_PRICE_COLUMNS_TO_TYPE.get(column_name)
    )
    return POSTGRES_TYPE_TO_PYTHON_TYPE[x]


def create_company_table(
    connection: psycopg2.connect,
    command: Optional[str] = None,
    table_name: str = "company",
    foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None,
) -> None:
    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (
                f"foreign key ({foreign_key_ref_tuple[0]}) references "
                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})"
            )
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

        # Create index for symbol column
        indexes = [
            f"CREATE INDEX idx_{table_name}_symbol ON {table_name}(symbol);"
        ]

    try:
        cursor = connection.cursor()
        cursor.execute(command)

        # Create indexes if not using the default command
        if command is None:
            for index_cmd in indexes:
                cursor.execute(index_cmd)

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_income_statement_table(
    connection: psycopg2.connect,
    command: Optional[str] = None,
    table_name: str = "income_statement_fy",
    foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None,
) -> None:
    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (
                f"foreign key ({foreign_key_ref_tuple[0]}) references "
                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})"
            )
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

        # Create indexes for company_id, symbol, and date columns
        indexes = [
            f"CREATE INDEX idx_{table_name}_company_id ON {table_name}(company_id);",
            f"CREATE INDEX idx_{table_name}_symbol ON {table_name}(symbol);",
            f"CREATE INDEX idx_{table_name}_date ON {table_name}(date);"
        ]

    try:
        cursor = connection.cursor()
        cursor.execute(command)

        # Create indexes if not using the default command
        if command is None:
            for index_cmd in indexes:
                cursor.execute(index_cmd)

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_balance_sheet_table(
    connection: psycopg2.connect,
    command: Optional[str] = None,
    table_name: str = "balance_sheet_fy",
    foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None,
) -> None:
    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (
                f"foreign key ({foreign_key_ref_tuple[0]}) references "
                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})"
            )
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

        # Create indexes for company_id, symbol, and date columns
        indexes = [
            f"CREATE INDEX idx_{table_name}_company_id ON {table_name}(company_id);",
            f"CREATE INDEX idx_{table_name}_symbol ON {table_name}(symbol);",
            f"CREATE INDEX idx_{table_name}_date ON {table_name}(date);"
        ]

    try:
        cursor = connection.cursor()
        cursor.execute(command)

        # Create indexes if not using the default command
        if command is None:
            for index_cmd in indexes:
                cursor.execute(index_cmd)

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_cash_flow_statement_table(
    connection: psycopg2.connect,
    command: Optional[str] = None,
    table_name: str = "cash_flow_statement_fy",
    foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None,
) -> None:
    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (
                f"foreign key ({foreign_key_ref_tuple[0]}) references "
                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})"
            )
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

        # Create indexes for company_id, symbol, and date columns
        indexes = [
            f"CREATE INDEX idx_{table_name}_company_id ON {table_name}(company_id);",
            f"CREATE INDEX idx_{table_name}_symbol ON {table_name}(symbol);",
            f"CREATE INDEX idx_{table_name}_date ON {table_name}(date);"
        ]

    try:
        cursor = connection.cursor()
        cursor.execute(command)

        # Create indexes if not using the default command
        if command is None:
            for index_cmd in indexes:
                cursor.execute(index_cmd)

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_price_table(
    connection: psycopg2.connect,
    command: Optional[str] = None,
    table_name: str = "price",
    foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None,
) -> None:
    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_PRICE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"

        # Add foreign key constraint
        if foreign_key_ref_tuple is None:
            foreign_key_ref_tuple = ("company_id", "company", "id")

        foreign_key_info = (
            f"foreign key ({foreign_key_ref_tuple[0]}) references "
            f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})"
        )

        # Add unique constraint for company_id, symbol, date
        unique_constraint = "UNIQUE(company_id, symbol, date)"

        # Add indexes
        indexes = [
            f"CREATE INDEX idx_{table_name}_company_id ON {table_name}(company_id);",
            f"CREATE INDEX idx_{table_name}_symbol ON {table_name}(symbol);",
            f"CREATE INDEX idx_{table_name}_date ON {table_name}(date);"
        ]

        command = f"""
            CREATE TABLE {table_name} (
                {column_column_type},
                {foreign_key_info},
                {unique_constraint}
            )
        """

    try:
        cursor = connection.cursor()
        cursor.execute(command)

        # Create indexes if not using the default command
        if command is None:
            for index_cmd in indexes:
                cursor.execute(index_cmd)

        # Close communication with the PostgreSQL database server
        cursor.close()
        # Commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def add_columns_if_not_exists(conn, table_name, columns):
    with conn.cursor() as cur:
        for column_name, column_type in columns.items():
            # Check if the column exists
            cur.execute(
                sql.SQL(
                    """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name = %s
            """
                ),
                (table_name, column_name),
            )
            result = cur.fetchone()
            # If the column does not exist, add it
            if not result:
                cur.execute(
                    sql.SQL(
                        """
                    ALTER TABLE {} 
                    ADD COLUMN {} {}
                """
                    ).format(
                        sql.Identifier(table_name),
                        sql.Identifier(column_name),
                        sql.SQL(column_type),
                    )
                )
        conn.commit()

def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.

    Parameters
    ----------
    config_file : str
        Path to the JSON configuration file

    Returns
    -------
    Dict[str, Any]
        Dictionary containing the configuration
    """
    # If config_file is a relative path, make it relative to the current file
    if not os.path.isabs(config_file):
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file)

    with open(config_file, 'r') as f:
        return json.load(f)


def create_tables(tables: List[AvailableTables], connection, foreign_key_ref_tuple=None):
    """
    Create the specified tables in the database.

    Parameters
    ----------
    tables : List[AvailableTables]
        List of tables to create
    connection
        Database connection
    foreign_key_ref_tuple : tuple, optional
        Foreign key reference tuple (column, table, reference_column)
    """
    # Default foreign key reference to company table
    if foreign_key_ref_tuple is None:
        foreign_key_ref_tuple = ("company_id", "company", "id")

    # Create tables based on the enum values
    for table in tables:
        print(f"Creating table: {table.value}")

        if table == AvailableTables.COMPANY:
            create_company_table(connection, table_name=table.value)

        elif table == AvailableTables.INCOME_STATEMENT_FY:
            create_income_statement_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        elif table == AvailableTables.INCOME_STATEMENT_QUARTER:
            create_income_statement_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        elif table == AvailableTables.BALANCE_SHEET_FY:
            create_balance_sheet_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        elif table == AvailableTables.BALANCE_SHEET_QUARTER:
            create_balance_sheet_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        elif table == AvailableTables.CASH_FLOW_STATEMENT_FY:
            create_cash_flow_statement_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        elif table == AvailableTables.CASH_FLOW_STATEMENT_QUARTER:
            create_cash_flow_statement_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )

        # elif table == AvailableTables.SHARES_FY:
        #     create_shares_table(
        #         connection,
        #         table_name=table.value,
        #         foreign_key_ref_tuple=foreign_key_ref_tuple
        #     )

        elif table == AvailableTables.PRICE:
            create_price_table(
                connection,
                table_name=table.value,
                foreign_key_ref_tuple=foreign_key_ref_tuple
            )


def driver(config_file: str = "config.json", tables: Optional[List[str]] = None):
    """
    Driver function to create database tables.

    Parameters
    ----------
    config_file : str, default="config.json"
        Path to the JSON configuration file
    tables : List[str], optional
        List of table names to create. If None, creates all tables.

    Returns
    -------
    None
    """
    # Load configuration
    config = load_config(config_file)

    # Connect to the database
    connection = connect(config["database"])
    if not connection:
        raise ValueError(
            f"Failed to connect to database: {config['database']['host']}/{config['database']['database']}")

    print(f"Connected to database: {config['database']['host']}/{config['database']['database']}")

    # Determine which tables to create
    if tables is None:
        # Create all tables if none specified
        tables_to_create = list(AvailableTables)
    else:
        # Create only the specified tables
        tables_to_create = []
        for table_name in tables:
            try:
                # Try to get the enum by value
                table_enum = next(t for t in AvailableTables if t.value == table_name)
                tables_to_create.append(table_enum)
            except StopIteration:
                print(f"Warning: Table '{table_name}' is not recognized and will be skipped.")

    # Create the tables
    create_tables(tables_to_create, connection)

    print("Table creation completed.")
    connection.close()


if __name__ == "__main__":
    pass
