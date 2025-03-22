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

def setLogLevel(sc, level):
    from pyspark.sql import SparkSession
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel(level)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)

    host = sys.argv[1]
    port = int(sys.argv[2])
    print ('host', type(host), host, 'port', type(port), port)



    sc_bak = SparkContext.getOrCreate()
    sc_bak.stop()

    time.sleep(15)
    print ('Ready to work!')

    ctx = pyspark.SparkContext(appName = "Netcat Wordcount", master="local[*]")
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
        # explode turns each item in an array into a separate row
        explode(
            split(data.value, ' ')
        ).alias('stock_data')
    )



    query = stock\
    .writeStream\
    .outputMode('append')\
    .format('console')\
    .start()



    query.awaitTermination()

