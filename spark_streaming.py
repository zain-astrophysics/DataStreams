from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Initialize SparkContext and StreamingContext
sc = SparkContext("local[2]", "StockDataStreaming")
ssc = StreamingContext(sc, 5)  # Batch interval of 5 seconds

# Create a DStream by connecting to the socket server (localhost:1000)
stock_stream = ssc.socketTextStream("localhost", 1000)

# Define the function to process the incoming data
def process_stock_data(time, rdd):
    if not rdd.isEmpty():
        stock_data = rdd.collect()
        print(f"Time: {time}")
        for record in stock_data:
            symbol, price = record.split(',')
            print(f"Stock: {symbol}, Price: {price}")

# Process the incoming stock data
stock_stream.foreachRDD(process_stock_data)

# Start the streaming job
ssc.start()

# Wait for the job to finish
ssc.awaitTermination()
