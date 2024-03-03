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



def get_income_statement(stock):
    api_key = "e742e1b74aa082782e9c1af7f30de1ef"  # Replace with your actual API key
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