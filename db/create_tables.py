import psycopg2

from typing import Optional, Tuple


DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE = {
    "id": "serial primary key",
    "symbol": "text",
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
    "finalLink": "text"
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
        "finalLink": "text"
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
        "finalLink": "text"}

DEFAULT_CURRENT_PRICE_COLUMNS_TO_TYPE = {
    "company_id": "serial",
    "date": "date",
    "marketCap": "bigint",
}


def create_company_table(connection: psycopg2.connect, command: Optional[str] = None,
                         table_name: str = "company",
                         foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None) -> None:

    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_COMPANY_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (f"foreign key ({foreign_key_ref_tuple[0]}) references "
                                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})")
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = \
            f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

    try:
        cursor = connection.cursor()
        cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_income_statement_table(connection: psycopg2.connect, command: Optional[str] = None,
                                  table_name: str = "income_statement_fy",
                                  foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None) -> None:

    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_INCOME_STATEMENT_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (f"foreign key ({foreign_key_ref_tuple[0]}) references "
                                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})")
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = \
            f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

    try:
        cursor = connection.cursor()
        cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_balance_sheet_table(connection: psycopg2.connect, command: Optional[str] = None,
                                  table_name: str = "balance_sheet_fy",
                                  foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None) -> None:

    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_BALANCE_SHEET_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (f"foreign key ({foreign_key_ref_tuple[0]}) references "
                                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})")
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = \
            f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

    try:
        cursor = connection.cursor()
        cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_cash_flow_statement_table(connection: psycopg2.connect, command: Optional[str] = None,
                                  table_name: str = "cash_flow_statement_fy",
                                  foreign_key_ref_tuple: Optional[Tuple[str, str, str]] = None) -> None:

    if command is None:
        column_column_type = ""
        for column, column_type in DEFAULT_CASHFLOW_STATEMENT_TABLE_COLUMNS_TO_TYPE.items():
            column_column_type += ("," if column_column_type else "") + f"{column} {column_type}"
        if foreign_key_ref_tuple is not None:
            foreign_key_info = (f"foreign key ({foreign_key_ref_tuple[0]}) references "
                                f"{foreign_key_ref_tuple[1]}({foreign_key_ref_tuple[2]})")
        else:
            foreign_key_info = None
        foreign_key_info = "," + foreign_key_info if foreign_key_info is not None else ""

        command = \
            f"""
            CREATE TABLE {table_name} (
                    {column_column_type}
                    {foreign_key_info}
                )
            """

    try:
        cursor = connection.cursor()
        cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
