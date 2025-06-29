import hashlib
import bitarray
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import requests
import sys
from pyspark import SparkContext
import os

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

    def to_base64(self):
        """Encode the bit array to a Base64 string."""
        return base64.b64encode(self.bit_array.tobytes()).decode('utf-8')

    @staticmethod
    def from_base64(encoded_str, size, num_hashes):
        """Reconstruct the BloomFilter from a Base64 string."""
        def fix_padding(s):
            padding_length = (4 - len(s) % 4) % 4
            return s + '=' * padding_length

        bloom_filter = BloomFilter(size, num_hashes)
        padded_str = fix_padding(encoded_str)
        bloom_filter.bit_array = bitarray.bitarray()
        bloom_filter.bit_array.frombytes(base64.b64decode(padded_str))
        bloom_filter.bit_array = bloom_filter.bit_array[:size]
        return bloom_filter

def load_bad_words():
    url = "https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt"
    response = requests.get(url)
    if response.status_code == 200:
        return {line.split('\t')[0] for line in response.text.splitlines() if int(line.split('\t')[1]) <= -4}
    return set()

def process_batch(batch_df, batch_id, bloom_filter):
    # Extract words from the incoming batch and check each word
    results = batch_df.rdd.map(lambda row: (row['words'], row['words'] in bloom_filter))

    # Collect the results
    results_collected = results.collect()

    # Filter out sentences with bad words
    bad_words_in_sentence = False
    for word, is_bad in results_collected:
        if is_bad:
            bad_words_in_sentence = True
            break  # No need to check further, sentence has a bad word

    # If sentence has no bad words, pass it along
    if not bad_words_in_sentence:
        # This would be where you pass the sentence to another system or process
        print(f"Sentence is GOOD: {batch_df.collect()}")
    else:
        # Suppress the sentence (just don't print it in this case)
        print("Sentence is SUPPRESSED due to bad word(s)")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: drunk-speech.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    
    # Spark session and context setup
    sc = SparkContext(appName="BloomFilterExample", master="local[*]")
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel("WARN")

    # Load bad words and initialize BloomFilter
    bad_words = load_bad_words()
    bloom_filter = BloomFilter(size=2000, num_hashes=15)
    for word in bad_words:
        bloom_filter.add(word)

    # Save Bloom filter bit array to HDFS once at the beginning
    bit_array_base64 = bloom_filter.to_base64()
    with open("/home/user/zainabbas199166/DataStreams/bloom_filter.txt", "w") as f:
        f.write(bit_array_base64)

    # HDFS path
    hdfs_path = "/user/zainabbas199166/datastreams"
    os.system(f"hadoop fs -put ~/DataStreams/bloom_filter.txt {hdfs_path}")

    # Load the Bloom filter from HDFS
    hdfs_data = spark.read.text(hdfs_path).collect()[0][0]
    bloom_filter_from_hdfs = BloomFilter.from_base64(hdfs_data, size=2000, num_hashes=15)

    # Set up Spark streaming from socket
    data = spark.readStream.format('socket').option('host', host).option('port', port).load()
    test_words = data.select(explode(split(data.value, ' ')).alias('words'))

    # Process stream using the pre-loaded Bloom filter
    query = test_words.writeStream.foreachBatch(
        lambda batch_df, batch_id: process_batch(batch_df, batch_id, bloom_filter_from_hdfs)
    ).start()

    query.awaitTermination()
