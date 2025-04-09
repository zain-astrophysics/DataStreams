# -*- coding: utf-8 -*-
"""stock_feeder.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1ujrb_tTvVpFKfujWdhfeRBcHFNRM_21x
"""

import pandas as pd
import time
#from google.colab import drive
import sys
from datetime import datetime, timedelta
from datetime import date


df = pd.read_csv('stock_data.csv')
df['datetime'] = pd.to_datetime(df['datetime'])

df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
for _, row in df.iterrows():
    print(row['datetime'], row['stock'], row['close'], flush=True)
    time.sleep(2)







