import logging
import sqlite3

import yfinance as yf
import pandas as pd
import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)

from stocksList import nifty_50


def download_stock_data(symbol, start_date=None, end_date=None, interval='1d'):
    """
    Downloads stock data for a given symbol from Yahoo Finance.

    Args:
        symbol (str): Stock symbol or ticker.
        start_date (str or None): Start date in 'YYYY-MM-DD' format. If None, uses the available historical data.
        end_date (str or None): End date in 'YYYY-MM-DD' format. If None, uses the current date.
        interval (str): Interval for the data (e.g., '1d' for daily, '1h' for hourly).

    Returns:
        pandas.DataFrame: Stock data for the given symbol.

    """
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=end_date, interval=interval)
        return data
    except Exception as e:
        logging.error(f"Error occurred while downloading {symbol} data: {str(e)}")
        return None


def save_to_csv(df: pd.DataFrame, symbol: str, filepath: str) -> None:
    """
    Saves stock data to a CSV file.

    Args:
        df (pandas.DataFrame): Stock data.
        symbol (str): Stock symbol or ticker.
        filepath (str): Path to the directory where the CSV file will be saved.

    """
    try:
        os.makedirs(filepath, exist_ok=True)  # Create the directory if it doesn't exist
        filename = os.path.join(filepath, f'{symbol}.csv')
        df.to_csv(filename, index=True)
        logging.info(f"{symbol} data saved to {filename}.")
    except Exception as e:
        logging.error(f"Error occurred while saving {symbol} data to a CSV file: {str(e)}")


def save_to_database(df, symbol, conn):
    """
    Saves stock data to a database table.

    Args:
        df (pandas.DataFrame): Stock data.
        symbol (str): Stock symbol or ticker.
        conn: Database connection object.

    """
    try:
        table_name = symbol.replace(".", "_")  # Use the symbol as the table name
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logging.info(f"{symbol} data saved to the database.")
    except Exception as e:
        logging.error(f"Error occurred while saving {symbol} data to the database: {str(e)}")


def download_and_save_data(symbols, start_date=None, end_date=None, save_to_db=False,db_conn=None, csv_path=None):
    """
    Downloads stock data for multiple symbols and saves them to either a database or CSV files.

    Args:
        symbols (list): List of stock symbols to download.
        start_date (str or None): Start date in 'YYYY-MM-DD' format. If None, uses the available historical data.
        end_date (str or None): End date in 'YYYY-MM-DD' format. If None, uses the current date.
        save_to_db (bool): Flag indicating whether to save the data to a database.
        db_conn: Database connection object.
        csv_path (str): Path to the directory where the CSV files will be saved.
    Returns:
        data (dict): containing downloaded data
    """
    data = {}
    for symbol in symbols:
        print(f"Downloading {symbol} data...")
        df = download_stock_data(symbol, start_date, end_date)
        if df is not None:
            data[symbol] = df
            if save_to_db:
                save_to_database(df, symbol, db_conn)
            if csv_path is not None:
                save_to_csv(df, symbol, csv_path)
    return data


def main():
    save_option = int(input('Do you want to save to the database? Enter 1 for saving to the database or 0 for saving to CSV.\n:  '))

    if save_option == 1:
        # Connect to the SQLite database
        try:
            conn = sqlite3.connect("stock_data.db")  # Replace with your desired database name
        except Exception as e:
            logging.error(f"Error occurred while connecting to the database: {str(e)}")
            return

        start_date = None  # Example start date (set to None to use the available historical data)
        end_date = None  # Example end date (set to None to use the current date)

        # Iterate over the Nifty 50 symbols and download/save data
        nifty50_symbols = nifty_50  # Add all the Nifty 50 symbols
        download_and_save_data(nifty50_symbols, start_date, end_date, save_to_db=True, db_conn=conn)

        # Close the database connection
        try:
            conn.close()
        except Exception as e:
            logging.error(f"Error occurred while closing the database connection: {str(e)}")

    else:
        start_date = None  # Example start date (set to None to use the available historical data)
        end_date = None  # Example end date (set to None to use the current date)
        csv_path = '/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/daily'
        symbols = nifty_50
        download_and_save_data(symbols, start_date, end_date, save_to_db=False, csv_path=csv_path)


if __name__ == '__main__':
    main()
