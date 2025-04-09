import hashlib
import bitarray
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import BooleanType
import requests
import sys
from pyspark import SparkContext

class BloomFilter:
    def __init__(self, size, num_hashes):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = bitarray.bitarray(size)
        self.bit_array.setall(0)

    def _hashes(self, word):
        return [int(hashlib.md5((word + str(i)).encode()).hexdigest(), 16) % self.size for i in range(self.num_hashes)]

    def add(self, word):
        for hash_val in self._hashes(word):
            self.bit_array[hash_val] = 1

    def __contains__(self, word):
        return all(self.bit_array[hash_val] for hash_val in self._hashes(word))

def load_bad_words():
    url = "https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt"
    response = requests.get(url)
    if response.status_code == 200:
        return {line.split('\t')[0] for line in response.text.splitlines() if int(line.split('\t')[1]) <= -4}
    return set()

def process_batch(batch_df, batch_id, bloom_filter):
    processed_df = batch_df.withColumn('is_bad', udf(lambda word: word in bloom_filter.value, BooleanType())(batch_df['words']))
    processed_df.select('words', 'is_bad').show()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <hostname> <port>")
        sys.exit(-1)

    host, port = sys.argv[1], int(sys.argv[2])
    sc = SparkContext(appName="BloomFilterExample")
    spark = SparkSession(sc)
    sc.setLogLevel("WARN")

    bad_words = load_bad_words()
    bloom_filter = BloomFilter(size=2000, num_hashes=5)
    for word in bad_words:
        bloom_filter.add(word)

    broadcast_bloom_filter = sc.broadcast(bloom_filter)
    data = spark.readStream.format('socket').option('host', host).option('port', port).load()
    test_words = data.select(explode(split(data.value, ' ')).alias('words'))

    query = test_words.writeStream.foreachBatch(
        lambda batch_df, batch_id: process_batch(batch_df, batch_id, broadcast_bloom_filter)
    ).start()
    query.awaitTermination()
