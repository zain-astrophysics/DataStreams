import requests
import pandas as pd

# MIN_REVIEWS_PER_ITEM = 25

url = 'https://raw.githubusercontent.com/singhj/big-data-repo/refs/heads/main/text-proc/reviews.csv'
response = requests.get(url)

import time

# Function to generate hash functions
def hash_fn_generator(n):
    M = 1119  # Prime number for modular arithmetic
    for i in range(n):
        def hash_function(x, i=i):  # Capture 'i' in the current loop iteration
            a = 2 * i + 1  # Coefficient 'a' (must be odd for better hashing)
            b = 100 * i  # Coefficient 'b'
            return (a * x + b + 2) % M
        yield hash_function

# Function to calculate MinHash values for a review text
def minhash(review, hash_functions):
    """
    Calculate MinHash values for a single review.

    Args:
        review (str): The review text.
        hash_functions (list): A list of hash functions.

    Returns:
        list: MinHash values for the review.
    """
    # Convert review text into shingles (individual words in this example)
    print(review)
    shingles = {hash(word) for word in review.split()}  # Hash words for uniformity

    # Calculate MinHash values for all hash functions
    minhash_values = []
    for hash_func in hash_functions:
        minhash_values.append(min(hash_func(shingle) for shingle in shingles))
    
    return minhash_values

# Example reviews (replace with your dataset)
reviews = cloth_data['Review Text']
reviews = [str(review) if isinstance(review, float) else review for review in reviews]

#eviews = 'zain is courageous and practice perserverance'

# Generate 200 hash functions
hash_functions = list(hash_fn_generator(200))

# Timing the MinHash computation for all reviews
start_time = time.time()

# Calculate MinHash values for each review
minhash_results = [minhash(review, hash_functions) for review in reviews]

end_time = time.time()

# Display MinHash results and computation time
for i, minhash_value in enumerate(minhash_results):
    print(f"Review {i + 1} MinHash values: {minhash_value}")

print(f"\nTime taken to compute MinHash values: {end_time - start_time:.4f} seconds")
