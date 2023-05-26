import datetime
from ..secrets.secrets5p import APP_NAME,APP_SOURCE,PASSWORD,ENCRYPTION_KEY,USER_ID,USER_KEY,client_code,Pin
from FivePaisaHelperLib import FivePaisaWrapper
from scraper import save_to_csv
from stocksList import nifty50_stocks

stocks_copy = nifty50_stocks
for i,symbol in enumerate(stocks_copy):
    stocks_copy[i] = symbol.split('.')[0]

app = FivePaisaWrapper(APP_NAME=APP_NAME,APP_SOURCE=APP_SOURCE,USER_KEY=USER_KEY,
                       USER_ID=USER_ID,PASSWORD=PASSWORD,ENCRYPTION_KEY=ENCRYPTION_KEY,client_code=client_code,pin=Pin)

app.login('Enter totp here')
app.load_conv_dict('/home/yeashu/project/AlgoTrading app/scrips/symbols2Scip.csv')

start = datetime.datetime(2019,1,1)
end = datetime.datetime(2023,5,25)
csvFilepath = '/home/yeashu/project/nifty data download/Data/Equities_csv/intraday'

data = app.download_intraday_data(nifty50_stocks,'15m',start=start,end=end,verbose=True)

for symbol,df in data.items():
    print(f'Saving {symbol} data to csv')
    save_to_csv(df,symbol,filepath=csvFilepath)