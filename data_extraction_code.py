import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import os
import datetime
from datetime import date
import uuid

def runner():
    # Replace with your Alpha Vantage API key
    api_key = '5K2YILCJ5PZQJ6RW'
    ticker = 'AAPL'  # Replace with the stock symbol you want to fetch

    # Initialize Alpha Vantage API
    ts = TimeSeries(key=api_key, output_format='pandas')

    # Get today's date
    today = date.today()
    
    # Fetch the daily stock data
    data, meta_data = ts.get_daily(symbol=ticker, outputsize='compact')
    
    # Extract relevant columns and create a DataFrame
    df = pd.DataFrame(data).loc[:, ['1. open', '2. high', '3. low', '4. close', '5. volume']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # Generate a unique filename
    filename = str(uuid.uuid4())
    
    # Define the output file path
    output_file = "/home/ubuntu/{}.parquet".format(filename)

    # Write the DataFrame to a Parquet file
    df.to_parquet(output_file)

    # Return the path to the Parquet file
    return output_file


