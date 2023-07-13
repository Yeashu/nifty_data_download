import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import datetime
import pandas as pd

from lib.FivePaisaHelperLib import FivePaisaWrapper
from scraper import download_stock_data, save_to_csv
from stocksList import nifty50_stocks


def load_dataframe_from_csv(symbol: str, path: str, index_col: str = 'Date') -> pd.DataFrame:
    """
    Load a DataFrame from a CSV file.

    Args:
        symbol (str): The symbol or stock name.
        path (str): The path to the CSV file.
        index_col (str, optional): The column to use as the index. Default is 'Date'.

    Returns:
        pd.DataFrame: The loaded DataFrame from the CSV file.
    """
    try:
        return pd.read_csv(f'{path}/{symbol}.csv', index_col=index_col)
    except FileNotFoundError:
        return pd.DataFrame()


def UpdateYfData(save_location: str, data_read_location: str) -> None:
    """
    Update Yahoo Finance data for Nifty 50 stocks and save it to CSV files.

    Args:
        save_location (str): The location to save the updated data.
        data_read_location (str): The location to read the existing data.

    Returns:
        None
    """
    histdata = {stock: load_dataframe_from_csv(stock, data_read_location) for stock in nifty50_stocks}
    dData = {stock: download_stock_data(stock + '.NS', None, None) for stock in nifty50_stocks}
    for stock, values in dData.items():
        histdata[stock] = pd.concat([histdata[stock], values])
    for symbol, df in histdata.items():
        save_to_csv(df=df, symbol=symbol, filepath=save_location)


def UpdateData(
    app: FivePaisaWrapper,
    totp: str,
    save_location: str,
    data_read_location: str,
    intraday: bool = True,
    updateFrom: datetime.datetime = None
) -> dict:
    """
    Update data for Nifty 50 stocks using the 5paisa API and save it to CSV files.

    Args:
        app (FivePaisaWrapper): The FivePaisaWrapper object.
        totp (str): The TOTP (Time-based One-Time Password) for authentication.
        save_location (str): The location to save the updated data.
        data_read_location (str): The location to read the existing data.
        intraday (bool, optional): Whether to update intraday data. Default is True.
        updateFrom (datetime.datetime, optional): The date and time from which to update the data. Default is None.
            If not provided, the function will check the latest available data and start from there.

    Returns:
        dict: A dictionary containing the updated data for each symbol.
    """
    app.login(totp)
    app.load_conv_dict(data_read_location)

    timeIndex = "Datetime" if intraday else "Date"

    data = {}
    csvFilepath = save_location

    try:
        symbol = nifty50_stocks[37]
        df = load_dataframe_from_csv(symbol, csvFilepath, index_col=timeIndex)
        if timeIndex in df.columns:
            updateFrom = pd.to_datetime(df[timeIndex].iloc[-1]).to_pydatetime()
            print(updateFrom)
    except Exception as e:
        print(f"An error occurred: {e}")

    start = updateFrom if updateFrom else datetime.datetime(2019, 1, 1)
    end = datetime.datetime.now()

    try:
        if intraday:
            data = app.download_intraday_data(
                nifty50_stocks, "15m", start=start, end=end, verbose=True
            )
        else:
            data = app.download(nifty50_stocks, '1d', start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"An error occurred: {e}")

    for symbol, df in data.items():
        print(f"Saving updated {symbol} data to csv")
        existing_data = load_dataframe_from_csv(symbol, csvFilepath, index_col=timeIndex)
        combined_data = pd.concat([existing_data, df])
        save_to_csv(combined_data, symbol, filepath=csvFilepath)

    return data
