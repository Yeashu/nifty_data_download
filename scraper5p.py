import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import datetime
import pandas as pd
from tqdm import tqdm

from lib.FivePaisaHelperLib import FivePaisaWrapper
from scraper import download_stock_data, save_to_csv

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
        return pd.read_csv(f'{path}\{symbol}.csv', index_col=index_col)
    except FileNotFoundError:
        return pd.DataFrame()


def UpdateYfData(symbols: list, save_location: str, data_read_location: str) -> None:
    """
    Update Yahoo Finance data for the specified symbols and save it to CSV files.

    Args:
        symbols (list): List of symbols or stock names.
        save_location (str): The location to save the updated data.
        data_read_location (str): The location to read the existing data.

    Returns:
        None
    """
    histdata = {stock: load_dataframe_from_csv(stock, data_read_location) for stock in symbols}
    dData = {stock: download_stock_data(stock + '.NS', None, None) for stock in tqdm(symbols, desc="Downloading data")}
    for stock, values in dData.items():
        histdata[stock] = pd.concat([histdata[stock], values])
    for symbol, df in tqdm(histdata.items(), desc="Saving data"):
        save_to_csv(df=df, symbol=symbol, filepath=save_location)


def UpdateData(
    app: FivePaisaWrapper,
    symbols: list,
    save_location: str,
    data_read_location: str,
    intraday: bool = True,
    updateFrom: datetime.datetime = None
) -> dict:
    """
    Update data for the specified symbols using the 5paisa API and save it to CSV files.

    Args:
        app (FivePaisaWrapper): The FivePaisaWrapper object.
        symbols (list): List of symbols or stock names.
        totp (str): The TOTP (Time-based One-Time Password) for authentication.
        save_location (str): The location to save the updated data.
        data_read_location (str): The location to read the existing data.
        intraday (bool, optional): Whether to update intraday data. Default is True.
        updateFrom (datetime.datetime, optional): The date and time from which to update the data. Default is None.
            If not provided, the function will check the latest available data and start from there.

    Returns:
        dict: A dictionary containing the updated data for each symbol.
    """

    timeIndex = "Datetime" if intraday else "Date"

    data = {}
    csvFilepath = data_read_location

    try:
        if updateFrom is None:
            symbol = symbols[0]
            df = load_dataframe_from_csv(symbol, csvFilepath, index_col=timeIndex)
            if timeIndex == df.index.name:
                updateFrom = pd.to_datetime(df.index[-1]).to_pydatetime()
    except Exception as e:
        print(f"An error occurred: {e}")

    start = updateFrom if updateFrom else datetime.datetime(2019, 1, 1)
    input(f'Update Date is {start} Continue')
    end = datetime.datetime.now()

    try:
        if intraday:
            data = app.download_intraday_data(
                symbols, "15m", start=start, end=end, verbose=False
            )
        else:
            data = app.download(symbols, '1d', start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        for symbol, df in tqdm(data.items(), desc="Saving data"):
            existing_data = load_dataframe_from_csv(symbol, csvFilepath, index_col=timeIndex)
            combined_data = pd.concat([existing_data, df]).drop_duplicates(keep='first')
            save_to_csv(combined_data, symbol, filepath=save_location)

    except Exception as e:
        print(f"An error occurred: {e}")

    return data

def update_or_download_data(app: FivePaisaWrapper, symbols: list, saveDir: str, loadDir: str,update:bool = True) -> None:
    """
    Updates existing data or downloads missing data for the specified symbols.

    Args:
        app (FivePaisaWrapper): The FivePaisaWrapper object.
        symbols (list): List of symbols or stock names.
        saveDir (str): The directory to save the updated or downloaded data.
        loadDir (str): The directory to check for existing CSV files.

    Returns:
        None
    """
    existing_symbols = []

    for symbol in tqdm(symbols, desc="Checking existing data"):
        csv_file_path = os.path.join(loadDir, f"{symbol}.csv")
        if os.path.isfile(csv_file_path):
            existing_symbols.append(symbol)

    if existing_symbols:
        if update:
            UpdateData(app, existing_symbols, saveDir, loadDir)

    missing_symbols = list(set(symbols) - set(existing_symbols))

    if missing_symbols:
        start_date = datetime.datetime(2019, 1, 1)
        end_date = datetime.datetime.now()
        data = app.download_intraday_data(missing_symbols, "15m", start=start_date, end=end_date, verbose=False)

        for symbol, df in tqdm(data.items(), desc="Downloading missing data"):
            save_to_csv(df, symbol, filepath=saveDir)
