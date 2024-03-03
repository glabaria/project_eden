# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 18:14:45 2024

@author: labar
"""


import bs4 as bs
import requests
import pandas as pd
import time
import os

def get_sp500_tickers():
    resp = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = bs.BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []

    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip()
        tickers.append(ticker)

    return tickers

sp500_tickers = get_sp500_tickers()
print(sp500_tickers)

api_key = "e742e1b74aa082782e9c1af7f30de1ef"

def get_income_statement(stock):  # Replace with your actual API key
    income_statement = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{stock}?period=annual&limit=400&apikey={api_key}")
    income_statement = income_statement.json()
    return income_statement

# Create a folder to store CSV files
if not os.path.exists("income_statements"):
    os.makedirs("income_statements")

# Iterate through tickers and retrieve income statements
for i, ticker in enumerate(sp500_tickers, start=1):
    income_statement_data = get_income_statement(ticker)
    # Process the income statement data as needed
    # (e.g., extract relevant information, perform calculations, etc.)
    # You can save it directly to a CSV file named after the ticker symbol
    filename = f"income_statements/{ticker}_income_statement.csv"
    pd.DataFrame(income_statement_data).to_csv(filename, index=False)
    print(f"Income statement for {ticker} saved to {filename}")

    # Introduce a sleep after every 300 iterations
    if i % 300 == 0:
        print(f"Sleeping for 1 minute after {i} iterations...")
        time.sleep(60)  # Sleep for 60 seconds (1 minute)

print("All income statements saved to separate CSV files in the 'income_statements' folder.")


#%% Balance Sheets

def get_balance_sheet(stock):
    balance_sheet = requests.get(f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{stock}?period=annual&limit=400&apikey={api_key}")
    balance_sheet = balance_sheet.json()
    return balance_sheet

# Create a folder to store CSV files
if not os.path.exists("balance_sheets"):
    os.makedirs("balance_sheets")

# Iterate through tickers and retrieve balance sheet statements
for i, ticker in enumerate(sp500_tickers, start=1):
    balance_sheet_data = get_balance_sheet(ticker)
    # Process the balance sheet data as needed
    # (e.g., extract relevant information, perform calculations, etc.)
    # You can save it directly to a CSV file named after the ticker symbol
    filename = f"balance_sheets/{ticker}_balance_sheet.csv"
    pd.DataFrame(balance_sheet_data).to_csv(filename, index=False)
    print(f"Balance sheet for {ticker} saved to {filename}")

    # Introduce a sleep after every 300 iterations
    if i % 300 == 0:
        print(f"Sleeping for 1 minute after {i} iterations...")
        time.sleep(60)  # Sleep for 60 seconds (1 minute)

print("All balance sheets saved to separate CSV files in the 'balance_sheets' folder.")


#%% Cash Flow Statements

def get_cash_flow(stock):
    cash_flow = requests.get(f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{stock}?period=annual&limit=400&apikey={api_key}")
    cash_flow = cash_flow.json()
    
    return cash_flow

# Create a folder to store CSV files
if not os.path.exists("cash_flows"):
    os.makedirs("cash_flows")

# Iterate through tickers and retrieve income statements
for i, ticker in enumerate(sp500_tickers, start=1):
    cash_flow_data = get_cash_flow(ticker)
    # Process the cash flow data as needed
    # (e.g., extract relevant information, perform calculations, etc.)
    # You can save it directly to a CSV file named after the ticker symbol
    filename = f"cash_flow/{ticker}_cash_flow.csv"
    pd.DataFrame(cash_flow_data).to_csv(filename, index=False)
    print(f"Cash Flow Statement for {ticker} saved to {filename}")

    # Introduce a sleep after every 300 iterations
    if i % 300 == 0:
        print(f"Sleeping for 1 minute after {i} iterations...")
        time.sleep(60)  # Sleep for 60 seconds (1 minute)

print("All Cash Flow Statements saved to separate CSV files in the 'cash_flows' folder.")



