# Nifty Data Download 📈

This repository is a submodule of the [AlgoTrading-app](https://github.com/Yeashu/AlgoTrading-app). It contains scripts for downloading and updating India stock market data from Yahoo Finance and 5Paisa, essential for the algo trading functionalities.

## Contents

- `Update.py`: Main script to initiate the data update process.
- `scraper5p.py`: Contains functions to download and update stock data from Yahoo Finance and 5Paisa.
- `lib/`: Directory containing helper libraries (e.g., `FivePaisaHelperLib`).

## Requirements

- Python 3.x
- pandas
- tqdm
- FivePaisaWrapper
- Other dependencies as required in the `lib/` directory

## Setup

1. Clone the main repository (AlgoTrading-app) and initialize the submodules:

    ```sh
    git clone --recurse-submodules https://github.com/Yeashu/AlgoTrading-app.git
    cd AlgoTrading-app
    ```

2. Install the required Python packages:

    ```sh
    pip install -r requirements.txt
    ```

3. Ensure the necessary libraries are available in the `lib/` directory.

## Usage

This repository is primarily used within the AlgoTrading-app. However, it can be used independently to update stock market data.

### Update Data

1. **Using Yahoo Finance Data**:

    To update Yahoo Finance data for specific symbols, modify the `Update.py` script with the desired symbols and execute:

    ```sh
    python Update.py
    ```

2. **Using 5Paisa Data**:

    Make sure you have the `FivePaisaWrapper` configured properly with your credentials and TOTP (Time-based One-Time Password) if required.

    Update the `Update.py` script with the necessary symbols and 5Paisa API details, then execute:

    ```sh
    python Update.py
    ```

## Script Details

### `Update.py`

This is the main script that initiates the data update process by calling functions from `scraper5p.py`.

```python
from scraper5p import UpdateData, UpdateYfData

if __name__ == '__main__':
    # Example usage for updating data
    UpdateData('')
    UpdateYfData()
```
### `scraper5p.py`

Contains functions to handle the downloading and updating of stock data.

-   `UpdateYfData(symbols, save_location, data_read_location)`: Updates data from Yahoo Finance for given symbols.
-   `UpdateData(app, symbols, save_location, data_read_location, intraday=True, updateFrom=None)`: Updates data using 5Paisa API for given symbols.
-   `load_dataframe_from_csv(symbol, path, index_col='Date')`: Loads a DataFrame from a CSV file.
-   `update_or_download_data(app, symbols, saveDir, loadDir, update=True)`: Updates existing data or downloads missing data for given symbols.

## Example Usage

```python

from scraper5p import UpdateData, UpdateYfData

if __name__ == '__main__':
    symbols = ['RELIANCE', 'TCS', 'INFY']
    save_location = 'data/save'
    data_read_location = 'data/read'

    # Example usage for Yahoo Finance data
    UpdateYfData(symbols, save_location, data_read_location)

    # Example usage for 5Paisa data
    from lib.FivePaisaHelperLib import FivePaisaWrapper
    app = FivePaisaWrapper(api_key='your_api_key', secret_key='your_secret_key', totp='your_totp')
    UpdateData(app, symbols, save_location, data_read_location)
```

