import hashlib
import math
from collections import deque

class SlidingWindowBloomFilter:
    def __init__(self, window_size, num_bits, num_hashes):
        """
        Initializes the sliding window bloom filter.
        
        Args:
        window_size: Number of time steps in the window (number of buckets).
        num_bits: Number of bits in the bloom filter.
        num_hashes: Number of hash functions to use.
        """
        self.window_size = window_size
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self.bloom_filters = deque()  # A deque of bloom filters for each time window
        self.bit_array = [0] * num_bits  # The main bit array shared across all buckets
        self.hash_seeds = self._generate_seeds(num_hashes)
        
        # Initialize deque with empty bloom filters
        for _ in range(window_size):
            self.bloom_filters.append([0] * num_bits)
    
    def _generate_seeds(self, num_hashes):
        """Generates random seeds for the hash functions."""
        return [i for i in range(num_hashes)]
    
    def _hash(self, item, seed):
        """Hashes an item using SHA-256 and returns an index based on the seed."""
        item = str(item).encode('utf-8')
        hash_value = int(hashlib.sha256(item + str(seed).encode('utf-8')).hexdigest(), 16)
        return hash_value % self.num_bits

    def add(self, item):
        """Adds an item to the current time window."""
        current_filter = self.bloom_filters[-1]  # The most recent bucket
        
        # Add the item to the bloom filter by setting bits in the current window
        for seed in self.hash_seeds:
            idx = self._hash(item, seed)
            current_filter[idx] = 1
            self.bit_array[idx] = 1
    
    def query(self, item):
        """Checks if an item is in the bloom filter."""
        for seed in self.hash_seeds:
            idx = self._hash(item, seed)
            if self.bit_array[idx] == 0:
                return False  # Definitely not in the filter
        return True  # Might be in the filter
    
    def advance_window(self):
        """Advances the sliding window, expiring the oldest filter and creating a new one."""
        # Expire the oldest filter
        oldest_filter = self.bloom_filters.popleft()
        for i in range(self.num_bits):
            if oldest_filter[i] == 1:
                self.bit_array[i] = 0
        
        # Add a new empty bloom filter for the new time step
        new_filter = [0] * self.num_bits
        self.bloom_filters.append(new_filter)

# Example usage
if __name__ == "__main__":
    window_size = 5  # Time window of 5 buckets
    num_bits = 100  # Bloom filter size
    num_hashes = 3  # Number of hash functions

    sw_bloom_filter = SlidingWindowBloomFilter(window_size, num_bits, num_hashes)
    
    # Add items
    sw_bloom_filter.add("item1")
    sw_bloom_filter.add("item2")
    
    # Query items
    print(sw_bloom_filter.query("item1"))  # Might return True
    print(sw_bloom_filter.query("item3"))  # Should return False
    
    # Advance the sliding window
    sw_bloom_filter.advance_window()
    
    # Add new items and query again
    sw_bloom_filter.add("item3")
    print(sw_bloom_filter.query("item2"))  # Might still return True
    print(sw_bloom_filter.query("item3"))  # Might return True
