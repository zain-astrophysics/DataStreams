import hashlib
import bitarray
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split
from pyspark.sql.types import BooleanType
import requests
import sys
import time
from pyspark import SparkContext

def setLogLevel(sc, level):
    from pyspark.sql import SparkSession
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel(level)

# Define the Bloom Filter class
class BloomFilter:
    def __init__(self, size: int, num_hashes: int):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = bitarray.bitarray(size)
        self.bit_array.setall(0)

    def _hashes(self, word: str):
        """Generate a list of hash values for a given word."""
        hash_values = []
        for i in range(self.num_hashes):
            hash_val = int(hashlib.md5((word + str(i)).encode('utf-8')).hexdigest(), 16)
            hash_values.append(hash_val % self.size)
        return hash_values

    def add(self, word: str):
        """Add a word to the Bloom Filter."""
        for hash_val in self._hashes(word):
            self.bit_array[hash_val] = 1

    def __contains__(self, word: str):
        """Check if a word is in the Bloom Filter."""
        return all(self.bit_array[hash_val] for hash_val in self._hashes(word))

# Load AFINN bad words
def load_bad_words():
    url = "https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt"
    response = requests.get(url)
    bad_words = set()
    if response.status_code == 200:
        content = response.text
        for line in content.splitlines():
            word, rating = line.strip().split('\t')
            if int(rating) <= -4:  # We only care about words with rating -4 or -5
                bad_words.add(word)
    return bad_words

# Function to process each batch of incoming data
def process_batch(batch_df, batch_id, bloom_filter):
    # Apply the UDF to add an 'is_bad' column
    processed_df = batch_df.withColumn('is_bad', is_bad_word_udf(batch_df['words']))
    
    # Output the results to the console
    processed_df.select('words', 'is_bad').write.format('console').outputMode('append')

# UDF to check if a word is in the Bloom Filter
def is_bad_word(word):
    # Access the broadcasted Bloom Filter
    return word in bloom_filter.value

# Register UDF with Spark
# We will register this UDF later when broadcasting the Bloom filter
is_bad_word_udf = udf(is_bad_word, BooleanType())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: drunk-speech.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    print('Argv', sys.argv)

    host = sys.argv[1]
    port = int(sys.argv[2])
    print('host', type(host), host, 'port', type(port), port)

    # Initialize Spark context
    sc = SparkContext(appName="BloomFilterExample", master="local[*]")
    spark = SparkSession(sc)

    # Set logging level
    setLogLevel(sc, "WARN")

    # Load bad words from AFINN and initialize Bloom filter
    bad_words = load_bad_words()
    bloom_filter = BloomFilter(size=2000, num_hashes=5)

    # Add bad words to the Bloom filter
    for word in bad_words:
        bloom_filter.add(word)

    # Broadcast the Bloom filter
    broadcast_bloom_filter = sc.broadcast(bloom_filter)

    # Register the UDF to use the broadcasted Bloom filter
    is_bad_word_udf = udf(lambda word: word in broadcast_bloom_filter.value, BooleanType())

    # Create DataFrame representing the stream of input lines from connection to host:port
    data = spark.readStream.format('socket').option('host', host).option('port', port).load()

    # Extract words from the incoming stream
    test_words = data.select(explode(split(data.value, ' ')).alias('words'))
    # Write stream with processing batch
    query = test_words.writeStream.foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, broadcast_bloom_filter)).outputMode('append').start()

    query.awaitTermination()
