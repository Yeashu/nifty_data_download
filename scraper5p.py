import datetime
import os

import pandas as pd
from ApiKeys.secrets5p import (
    APP_NAME,
    APP_SOURCE,
    PASSWORD,
    ENCRYPTION_KEY,
    USER_ID,
    USER_KEY,
    client_code,
    Pin,
)
from lib.FivePaisaHelperLib import FivePaisaWrapper
from scraper import save_to_csv,download_and_save_data
from stocksList import nifty50_stocks

app = FivePaisaWrapper(
    APP_NAME=APP_NAME,
    APP_SOURCE=APP_SOURCE,
    USER_KEY=USER_KEY,
    USER_ID=USER_ID,
    PASSWORD=PASSWORD,
    ENCRYPTION_KEY=ENCRYPTION_KEY,
    client_code=client_code,
    pin=Pin,
)


def UpdateData(totp:str,intraday:bool=True):
    app.login(totp)
    app.load_conv_dict(
        "/home/yeashu/project/AlgoTrading app/scrips/symbols2Scip.csv"
    )
    csvFilepath = "/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/daily/nifty50"
    timeIndex = "Date"
    if intraday:
        csvFilepath = "/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/intraday"
        timeIndex = "Datetime"
    latest_date = 0

    symbol = nifty50_stocks[3]
    df = pd.read_csv(f'{csvFilepath}/{symbol}.csv')
    if timeIndex in df.columns:
        latest_date = pd.to_datetime(df[timeIndex].iloc[-1]).to_pydatetime()
    

    if latest_date == 0:
        c = input("Cannot find date automatically do you want to continue[yes/no]")
        if 'n' in c:
            return
        latest_date = input("Enter date manually deafaults to 2019-01-01 : ")
    # Set the start date for updating the data
    start = latest_date if latest_date else datetime.datetime(2019, 1, 1)
    end = datetime.datetime.now()  # Set the end date as the current date and time

    if intraday:
        data = app.download_intraday_data(
            nifty50_stocks, "15m", start=start, end=end, verbose=True
        )
    else :
        #Using yfinance
        c_nifty50_stocks = nifty50_stocks
        for i,symbol in enumerate(c_nifty50_stocks):
            c_nifty50_stocks[i] = symbol+'.NS'
        data = download_and_save_data(c_nifty50_stocks,start=start,end_date=end)

    for symbol, df in data.items():
        print(f"Saving updated {symbol} data to csv")
        symbol_file = os.path.join(csvFilepath, f"{symbol}.csv")
        existing_data = pd.read_csv(symbol_file) if os.path.isfile(symbol_file) else pd.DataFrame()
        combined_data = pd.concat([existing_data, df])
        save_to_csv(combined_data, symbol, filepath=csvFilepath) 


if __name__ == "__main__":
    print("To update data enter 1 , To download data enter 2 and enter 0 to exit")
    input = int(input(": "))
    if input == 1:
        UpdateData("Enter totp here")
    elif input == 2:
        app.login("Enter totp here")
        app.load_conv_dict(
            "/home/yeashu/project/AlgoTrading app/scrips/symbols2Scip.csv"
        )

        start = datetime.datetime(2019, 1, 1)
        end = datetime.datetime(2023, 5, 25)
        csvFilepath = (
            "/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/intraday"
        )

        data = app.download_intraday_data(
            nifty50_stocks, "15m", start=start, end=end, verbose=True
        )

        for symbol, df in data.items():
            print(f"Saving {symbol} data to csv")
            save_to_csv(df, symbol, filepath=csvFilepath)
