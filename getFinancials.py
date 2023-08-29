import pandas as pd
import numpy as np
import yfinance as yf
from multiprocessing import Pool
import os
import csv
import time
import requests
from yahoofinancials import YahooFinancials
from sklearn.linear_model import LinearRegression


df_detailed = pd.read_csv('results.csv')
tickers = list(df_detailed["Symbol"])

def get_beta(ticker):
    symbols = [ticker, 'SPY']
    data = yf.download(symbols, '2020-2-22')['Adj Close']

    # Convert historical stock prices to daily percent change
    price_change = data.pct_change()

    # Deletes row one containing the NaN
    df = price_change.drop(price_change.index[0])

    # Create arrays for x and y variables in the regression model
    # Set up the model and define the type of regression
    x = np.array(df[ticker]).reshape((-1,1))
    y = np.array(df['SPY'])
    model = LinearRegression().fit(x, y)

    return model.coef_[0]

def get_roe(ticker):
    return yf.Ticker(ticker).info["returnOnEquity"]
with open('results_for_real.csv', 'a', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
    # Write the ticker and the data as a row
        writer.writerow(['ticker', 'beta', 'dy', 'pe', 'roe'])
def get_financial_data(ticker):
    # Use a local variable to store the data
    ticker = ticker.replace(" ", "")
    NOTPRESENT = float("Nan") 
    data = {}
    beta = NOTPRESENT 
    dy = NOTPRESENT
    pe = NOTPRESENT
    roe = NOTPRESENT
    financials = []
    summary_data = []
    try:
        financials = YahooFinancials(ticker)
        summary_data = financials.get_summary_data()
    except Exception as e: 
        return ticker,e
    try:
        beta = get_beta(ticker)        
    except Exception as e:
        return ticker, e
    try:
        dy = summary_data[ticker]["dividendYield"]
    except:
        pass
    try:
        pe= summary_data[ticker]['forwardPE']
    except Exception as e:
        return ticker, e 
    try: 
        roe=get_roe(ticker) 
    except:
        pass

    with open('results_for_real.csv', 'a', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
    # Write the ticker and the data as a row
        writer.writerow([ticker, beta, dy, pe, roe])
    # Return the ticker and a success message
    return ticker, "Success"
    

if __name__ == '__main__':
    
    results = []

    # Create a pool of processes with the number of CPU cores
    pool = Pool(os.cpu_count())

    # Map the function to the ticker list and get the results
    results = pool.map(get_financial_data, tickers)

    # Close and join the pool
    pool.close()
    pool.join()

    # Print a message when done
    print("All processes finished.")
