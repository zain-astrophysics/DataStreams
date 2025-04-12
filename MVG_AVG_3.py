from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_timestamp, concat_ws
import sys
import time
from datetime import datetime, timedelta

#Define the StockDetector class
class StockDetector:
    def __init__(self, short_window=10, long_window=40):
        self.prices = []  # list of (timestamp, price)
        self.short_window = short_window
        self.long_window = long_window
        self.last_signal = None  # To prevent repeated signals

    def update(self, new_data):
        # Add new rows to buffer
        for row in new_data:
            self.prices.append((row["Timestamp"], row["Price"]))

        # Remove old data beyond 40 minutes
        cutoff = datetime.utcnow() - timedelta(minutes=self.long_window)
        self.prices = [(ts, p) for ts, p in self.prices if ts >= cutoff]

        # Sort just in case (Spark doesn't guarantee order)
        self.prices.sort()

        # Compute moving averages
        short_cutoff = datetime.utcnow() - timedelta(minutes=self.short_window)
        short_prices = [p for ts, p in self.prices if ts >= short_cutoff]
        long_prices = [p for ts, p in self.prices]

        if len(short_prices) == 0 or len(long_prices) == 0:
            return None  # Not enough data yet

        short_ma = sum(short_prices) / len(short_prices)
        long_ma = sum(long_prices) / len(long_prices)

        # Detect crossover
        if short_ma > long_ma and self.last_signal != "buy":
            self.last_signal = "buy"
            return ("buy", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "AAPL")
        elif short_ma < long_ma and self.last_signal != "sell":
            self.last_signal = "sell"
            return ("sell", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "AAPL")
        else:
            return None  # No signal or same as last time

# 💡 Create Spark Streaming Job
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: stream_stock.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession.builder.appName("AAPLDetector").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Step 3: Read socket stream
    raw = spark.readStream.format("socket").option("host", host).option("port", port).load()

    # Step 4: Parse raw text into columns
    stock = raw.select(
        split(col("value"), " ").getItem(0).alias("Date"),
        split(col("value"), " ").getItem(1).alias("Time"),
        split(col("value"), " ").getItem(2).alias("Symbol"),
        split(col("value"), " ").getItem(3).cast("float").alias("Price")
    ).withColumn(
        "Timestamp",
        to_timestamp(concat_ws(" ", col("Date"), col("Time")), "yyyy-MM-dd HH:mm:ss")
    ).filter(col("Symbol") == "AAPL").select("Timestamp", "Symbol", "Price")

    # Step 5: Initialize the detector
    detector = StockDetector()

    # Step 6: Custom foreachBatch logic
    def process_batch(df, epoch_id):
        if df.isEmpty():
            return
        rows = df.collect()
        signal = detector.update(rows)
        if signal:
            print([signal])  # Print like [('buy', '2025-04-12 15:20:00', 'AAPL')]

    # Step 7: Attach foreachBatch
    query = stock.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()
