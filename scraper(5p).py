import sqlite3

import py5paisa

from secrets5p import (APP_NAME, APP_SOURCE, ENCRYPTION_KEY, PASSWORD, USER_ID,
                       USER_KEY)

cred = {
    "APP_NAME": APP_NAME,
    "APP_SOURCE": APP_SOURCE,
    "USER_ID": USER_ID,
    "PASSWORD": PASSWORD,
    "USER_KEY": USER_KEY,
    "ENCRYPTION_KEY": ENCRYPTION_KEY
}


client = py5paisa.FivePaisaClient(cred=cred)
client.get_access_token('Your Response Token')

# write a function to download data using 5paisa


def download_data(exchange, exchange_type, scrip_code, time_frame, from_date, to_date):
    # download candles data
    candles = client.historical_data(
        exchange, exchange_type, scrip_code, time_frame, from_date, to_date)
    return candles

# save data to a database


def save_data_to_db(candles, symbol):
    conn = sqlite3.connect('5paisa.db')
    # save data to a database
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {symbol} (open REAL, high REAL, low REAL, close REAL, volume REAL, date TEXT)")
    for candle in candles:
        conn.execute(f"INSERT INTO {symbol} VALUES (?, ?, ?, ?, ?, ?)",
                     (candle.open, candle.high, candle.low, candle.close, candle.volume, candle.date))
        conn.commit()

    conn.close()


if __name__ == "__main__":
    exchange = "N"
    exchange_type = "C"
    scrip_codes = [1660, 1234, 5678]  # Example list of scrip codes
    time_frame = "15m"
    from_date = "2023-01-01"
    to_date = "2023-01-02"

    for scrip_code in scrip_codes:
        candles = download_data(exchange, exchange_type,
                                scrip_code, time_frame, from_date, to_date)
        # You can replace this with the actual symbol if available
        symbol = str(scrip_code)
        save_data_to_db(candles, symbol)
        print(f"Data downloaded and saved for {symbol}")
