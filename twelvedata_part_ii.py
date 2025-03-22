# -*- coding: utf-8 -*-


import pandas as pd
import time
from datetime import datetime, timedelta
from twelvedata import TDClient

#  Initialize the TwelveData client
key = "68e63409a7c240939e093f3f981d2210"
td = TDClient(apikey=key)

#  Fetch overlapping data with 30-minute intervals every 15 minutes
def fetch_data_with_overlap(symbol, start_date, end_date, interval="15min"):

    all_data = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Increment in 1 year steps with a 30-minute overlap
    step = timedelta(days=365)
    overlap = timedelta(minutes=30)

    while current_date < end_date:
        fetch_start = current_date
        fetch_end = min(current_date + step + overlap, end_date)

        #print(f"Fetching {symbol} data from {fetch_start} to {fetch_end}...")

        try:
            data = td.time_series(
                symbol=symbol,
                interval=interval,
                start_date=fetch_start.strftime("%Y-%m-%d %H:%M:%S"),
                end_date=fetch_end.strftime("%Y-%m-%d %H:%M:%S"),
                outputsize=5000
            ).as_pandas()

            if not data.empty:
                data['stock'] = symbol
                all_data.append(data)
        except Exception as e:
            print(f"Failed to fetch data: {e}")

        # Move to the next batch
        current_date += step

        # Sleep to avoid rate limiting
        time.sleep(1)

    # Concatenate all fetched data
    if all_data:
        return pd.concat(all_data, ignore_index=False)
    else:
        return pd.DataFrame()

#  Fetch data for both stocks
aapl_data = fetch_data_with_overlap("AAPL", "2021-01-01", "2024-12-01")
msft_data = fetch_data_with_overlap("MSFT", "2021-01-01", "2024-12-01")

#  Combine and clean the data
if not aapl_data.empty and not msft_data.empty:
    # Reset index and sort by datetime column
    aapl_data = aapl_data.reset_index().sort_values(by='datetime')
    msft_data = msft_data.reset_index().sort_values(by='datetime')

    # Combine both stocks
    #all_data = pd.concat([aapl_data, msft_data], ignore_index=True)
    #all_data = all_data.sort_values(by='datetime')

    #  Display the data
    # print("\nCombined stock data:")
    # print(all_data.head())

    # Save to CSV
   # all_data.to_csv("stock_data_with_overlap.csv", index=False)

else:
    print("No data fetched.")

#for i in range(len(all_data)):
#    print(all_data.iloc[i])
#    time.sleep(2)

for _, row in aapl_data.iterrows():
    print(row['datetime'].date(), row['stock'], row['high'])
    time.sleep(2)





