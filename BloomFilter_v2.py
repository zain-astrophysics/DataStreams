import hashlib
import bitarray
import math
from pyspark import SparkContext
import sys, time
import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import *



def setLogLevel(sc, level):
    from pyspark.sql import SparkSession
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel(level)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: drunk-speech.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    print ('Argv', sys.argv)

    host = sys.argv[1]
    port = int(sys.argv[2])
    print ('host', type(host), host, 'port', type(port), port)


    sc_bak = SparkContext.getOrCreate()
    sc_bak.stop()

    time.sleep(15)
    print ('Ready to work!')

    ctx = pyspark.SparkContext(appName = "bloomFilter", master="local[*]")
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

# Function to load AFINN list and filter out words with -4 or -5 rating
def load_bad_words():
    bad_words = set()
    with open("https://raw.githubusercontent.com/fnielsen/afinn/master/afinn/data/AFINN-en-165.txt") as file:
        for line in file:
            word, rating = line.strip().split('\t')
            if int(rating) <= -4:  # We only care about words with rating -4 or -5
                bad_words.add(word)
    return bad_words

# Initialize Spark context
sc = SparkContext(appName="BloomFilterExample")

# Load the list of bad words from AFINN
bad_words = load_bad_words()

# Initialize Bloom Filter with 2000 bits and 5 hash functions (adjust as needed)
bloom_filter = BloomFilter(size=2000, num_hashes=5)

# Add bad words to Bloom filter
for word in bad_words:
    bloom_filter.add(word)

# Now we have the Bloom filter populated with bad words.
# Example check
#test_words = ["bad", "good", "worst", "best"]

test_words = data.select(
        split(data.value, ' ').getItem(0).alias('words'),       
   )

# Check words using the Bloom filter
results = [(word, word in bloom_filter) for word in test_words]

# Output the results (True means the word is likely a bad word, False means it isn't)
print(results)

# Stop Spark context
sc.stop()
