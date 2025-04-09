import hashlib
import bitarray
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import requests
import sys
from pyspark import SparkContext

class BloomFilter:
    def __init__(self, size: int, num_hashes: int):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = bitarray.bitarray(size)
        self.bit_array.setall(0)

    def _hashes(self, word: str):
        return [int(hashlib.md5((word + str(i)).encode('utf-8')).hexdigest(), 16) % self.size for i in range(self.num_hashes)]

    def add(self, word: str):
        for hash_val in self._hashes(word):
            self.bit_array[hash_val] = 1

    def __contains__(self, word: str):
        return all(self.bit_array[hash_val] for hash_val in self._hashes(word))

def load_bad_words():
    url = "https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt"
    response = requests.get(url)
    if response.status_code == 200:
        return {line.split('\t')[0] for line in response.text.splitlines() if int(line.split('\t')[1]) <= -4}
    return set()

def process_batch(batch_df, batch_id, bloom_filter):
    # Collect rows as a list
    rows = batch_df.collect()
    results = []
    for row in rows:
        word = row['words']
        is_bad = word in bloom_filter
        results.append((word, is_bad))
    
    # Display results
    for word, is_bad in results:
        print(f"{word} - {'BAD' if is_bad else 'GOOD'}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: drunk-speech.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    sc = SparkContext(appName="BloomFilterExample", master="local[*]")
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel("WARN")

    # Initialize BloomFilter with bad words
    bad_words = load_bad_words()
    bloom_filter = BloomFilter(size=2000, num_hashes=5)
    for word in bad_words:
        bloom_filter.add(word)

    # Create DataFrame representing the stream of input lines from connection to host:port
    data = spark.readStream.format('socket').option('host', host).option('port', port).load()

    # Extract words from the incoming stream
    test_words = data.select(explode(split(data.value, ' ')).alias('words'))

    # Write stream with processing logic
    query = test_words.writeStream.foreachBatch(
        lambda batch_df, batch_id: process_batch(batch_df, batch_id, bloom_filter)
    ).start()

    query.awaitTermination()
