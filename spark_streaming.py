import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import time

# Set up Spark context and streaming context
sc = SparkContext("local[2]", "StockDataStreaming")
ssc = StreamingContext(sc, 5)  # Stream data in batches of 5 seconds

# Define your Twelve Data API key
api_key = "YOUR_TWELVE_DATA_API_KEY"  # Replace with your actual API key

# Define the stocks you want to collect data for
stocks = ['AAPL', 'MSFT']

# Function to fetch stock data from Twelve Data API
def fetch_stock_data(symbol):
    url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=15min&apikey={api_key}'
    try:
        response = requests.get(url)
        data = response.json()
        if 'values' in data:
            # Extract the most recent stock price
            last_price = data['values'][0]['close']
            return f"{symbol},{last_price}"
        else:
            return None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

# Function to send data to the stream
def get_stream_data():
    stream_data = []
    for stock in stocks:
        stock_data = fetch_stock_data(stock)
        if stock_data:
            stream_data.append(stock_data)
    return "\n".join(stream_data)

# Set up a DStream to collect data from the socket (localhost:9999)
def send_to_stream():
    stream_data = get_stream_data()
    return stream_data

# Create an RDD from the stream data and send to Spark
def process_stream(rdd):
    if not rdd.isEmpty():
        stock_data = rdd.collect()
        for record in stock_data:
            print(f"Stock Data: {record}")

# Create a DStream from the socket
stock_stream = ssc.socketTextStream("localhost", 9999)

# Transform the incoming data into an RDD and process
stock_stream.foreachRDD(process_stream)

# Set up the logic to send data to the socket
ssc.start()

# Keep fetching and sending stock data to the socket at regular intervals
while True:
    stock_data = send_to_stream()
    if stock_data:
        # Simulate sending data to the socket (localhost:9999)
        with open('/tmp/stock_data.txt', 'w') as f:
            f.write(stock_data)
    time.sleep(10)  # Fetch new data every 10 seconds (adjust as needed)

# Wait for the stream to finish
ssc.awaitTermination()
