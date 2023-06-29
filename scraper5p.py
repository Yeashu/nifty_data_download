import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import datetime
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
from scraper import download_stock_data, save_to_csv
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

def UpdateYfData():
    dData = {}
    for stock in nifty50_stocks:
       symbol = stock + '.NS'
       dData[stock] = download_stock_data(symbol,None,None,) 
    path = '/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/daily/nifty50'
    histdata = {}
    for stock in nifty50_stocks:
        df = pd.read_csv(f'{path}/{stock}.csv',index_col='Date')
        histdata[stock] = df
    for stock, values in dData.items():
        histdata[stock] = pd.concat([histdata[stock],values])
    
    for symbol,df in histdata.items():
        save_to_csv(df=df,symbol=symbol,filepath=path)
    
def UpdateData(totp:str,intraday:bool=True,updateFrom:datetime.datetime=None):
    app.login(totp)
    app.load_conv_dict(
        "/home/yeashu/project/AlgoTrading app/scrips/symbols2Scip.csv"
    )
    csvFilepath = "/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/daily/5p"
    timeIndex = "Datetime"
    if intraday:
        csvFilepath = "/home/yeashu/project/AlgoTrading app/nifty_data_download/Data/Equities_csv/intraday"
        timeIndex = "Datetime"
    
    if not updateFrom:
        symbol = nifty50_stocks[37]
        df = pd.read_csv(f'{csvFilepath}/{symbol}.csv')
        if timeIndex in df.columns:
            updateFrom = pd.to_datetime(df[timeIndex].iloc[-1]).to_pydatetime()
            print(updateFrom)
    
    # Set the start date for updating the data
    start = updateFrom if updateFrom else datetime.datetime(2019, 1, 1)
    end = datetime.datetime.now()  # Set the end date as the current date and time

    if intraday:
        data = app.download_intraday_data(
            nifty50_stocks, "15m", start=start, end=end, verbose=True
        )
    else :
        data = app.download(nifty50_stocks,'1d',start.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"))

    for symbol, df in data.items():
        print(f"Saving updated {symbol} data to csv")
        symbol_file = os.path.join(csvFilepath, f"{symbol}.csv")
        existing_data = pd.read_csv(symbol_file,index_col=timeIndex) if os.path.isfile(symbol_file) else pd.DataFrame()
        combined_data = pd.concat([existing_data, df])
        save_to_csv(combined_data, symbol, filepath=csvFilepath)
    
    return data


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
