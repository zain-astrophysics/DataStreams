from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_timestamp, concat_ws
import sys
import time
from datetime import datetime, timedelta

#Define the StockDetector class

# Define a class to track stock data and detect crossovers
class StockDetector:
    # Initializes a new tracker for a specific stock symbol
    def __init__(self, symbol):
        self.symbol = symbol
        self.data = []
        self.last_signal_index = -1

    # Adding new data points to the data (have checked
    # whether they are added before by sorting it),
    # basically track the data from both Apple and Microsoft
    def data_point(self, new_data):
        self.data.extend(new_data)
        self.data.sort(key=lambda x: x[0])  # Sort by datetime

    # The signal calculation, which is used for both 10 and 40 days signal calculation
    def calculate_signals(self):
        signals = []

        # Initialize the basic
        ma_10day = []
        ma_40day = []

        # Here is the part of calculating both 10_day and 40_day MA
        for i in range(len(self.data)):
            if i >= 9:  # Need at least 10 data points for 10-day MA
                window_values = [self.data[j][1] for j in range(i-9, i+1)]
                ma_10day.append(sum(window_values) / 10)
            else:
                ma_10day.append(None)

            if i >= 39:  # Need at least 40 data points for 40-day MA
                window_values = [self.data[j][1] for j in range(i-39, i+1)]
                ma_40day.append(sum(window_values) / 40)
            else:
                ma_40day.append(None)

        # Only check for crossovers for new data points
        start_index = self.last_signal_index + 1

        # Detect crossovers
        for i in range(max(1, start_index), len(ma_10day)):
            if ma_10day[i] is not None and ma_40day[i] is not None and ma_10day[i-1] is not None and ma_40day[i-1] is not None:

                shares = int(100000 / self.data[i][1])  # Calculate number of shares to trade ($100K worth)

                # Golden cross: 10-day MA crosses above 40-day MA (buy signal)
                if ma_10day[i] > ma_40day[i] and ma_10day[i-1] <= ma_40day[i-1]:
                    signals.append(f"({self.data[i][0]} buy {self.symbol}) - {shares} shares at ${self.data[i][1]:.2f}")

                # Death cross: 10-day MA crosses below 40-day MA (sell signal)
                elif ma_10day[i] < ma_40day[i] and ma_10day[i-1] >= ma_40day[i-1]:
                    signals.append(f"({self.data[i][0]} sell {self.symbol}) - {shares} shares at ${self.data[i][1]:.2f}")

        # Update the last signal index
        self.last_signal_index = len(self.data) - 1
        return signals



# 💡 Create Spark Streaming Job
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: stream_twelvedata.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)

    host = sys.argv[1]
    port = int(sys.argv[2])
    print ('host', type(host), host, 'port', type(port), port)


    sc_bak = SparkContext.getOrCreate()
    sc_bak.stop()

    time.sleep(15)
    print ('Ready to work!')

    ctx = pyspark.SparkContext(appName = "stock_data", master="local[*]")
    print ('Context', ctx)

    spark = SparkSession(ctx).builder.getOrCreate()
    sc = spark.sparkContext

    setLogLevel(sc, "WARN")

    print ('Session:', spark)
    print ('SparkContext', sc)

      # Create DataFrame representing the stream of input lines from connection to host:port
    data = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    stock = data.select(
        split(data.value, ' ').getItem(0).alias('Date'),
        split(data.value, ' ').getItem(1).alias('Time'),
        split(data.value, ' ').getItem(2).alias('Symbol'),
        split(data.value, ' ').getItem(3).cast('float').alias('Price')
   )


# Concatenate timestamp

    stock_with_timestamp = stock.withColumn(
    'Timestamp',
    to_timestamp(concat_ws(' ', stock['Date'], stock['Time']), 'yyyy-MM-dd HH:mm:ss')
    )


# Filter for AAPL and MSFT prices
    aaplPrice = stock.filter(col("Symbol") == "AAPL").select('Timestamp', 'Symbol', 'Price')
    msftPrice = stock.filter(col("Symbol") == "MSFT").select('Timestamp', 'Symbol', 'Price')

    # Step 5: Initialize the detector
    detector = StockDetector()

    # Step 6: Custom foreachBatch logic
    def process_batch(df, epoch_id):
     if df.count() == 0:
        return
     rows = df.collect()
     signal = detector.update(rows)
     if signal:
        print([signal])

    # Step 7: Attach foreachBatch
    query = aaplPrice.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()
