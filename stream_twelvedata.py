# -*- coding: utf-8 -*-
"""stream_twelveData.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1OfNNi2v3U458ODDHK1_yJyH1lu5tYvlp
"""

import sys, time

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col

from pyspark.sql.functions import explode, split, col, avg, lag
from pyspark.sql.window import Window

def setLogLevel(sc, level):
    from pyspark.sql import SparkSession
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel(level)

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



 #   stock = data.select(
        # explode turns each item in an array into a separate row
 #       explode(
 #           split(data.value, ' ')
 #       ).alias('stock_data')
 #   )

    stock = data.select(
        split(data.value, ' ').getItem(0).alias('Date'),
        split(data.value, ' ').getItem(1).alias('Symbol'),
        split(data.value, ' ').getItem(2).cast('float').alias('Price')
   )

# Filter for AAPL prices
    aaplPrice = stock.filter(col("Symbol") == "AAPL")
    msftPrice = stock.filter(col("Symbol") == "MSFT")


# Define window specification for moving averages
    #windowSpec10 = Window.orderBy("Date").rowsBetween(-9, 0)  # 10-day window
    #windowSpec40 = Window.orderBy("Date").rowsBetween(-39, 0)  # 40-day window

    # Calculate 10-day and 40-day moving averages for AAPL stock
    #aaplWithMAs = aaplPrice.withColumn("10DayMA", avg("Price").over(windowSpec10)) \
     #                     .withColumn("40DayMA", avg("Price").over(windowSpec40))

    # Calculate Buy/Sell signals based on moving averages comparison
    #aaplSignals = aaplWithMAs.withColumn("Signal", 
     #                                   (col("10DayMA") > col("40DayMA")).cast("int") - (col("10DayMA") < col("40DayMA")).cast("int"))


    #query = stock\
    #.writeStream\
    #.outputMode('append')\
    #.format('console')\
    #.start()

    msftquery = msftPrice\
     .writeStream\
     .outputMode('append')\
     .format('console')\
     .start()
    
    aaplquery = aaplPrice\
    .writeStream\
    .outputMode('append')\
    .format('console')\
    .start()



    #query.awaitTermination()
    aaplquery.awaitTermination()
    msftquery.awaitTermination()
