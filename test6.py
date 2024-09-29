import math
from bitarray import bitarray
import hashlib


class CountingBloomFilter:
    def __init__(self, capacity, error_rate=0.001):
        self.capacity = capacity
        self.error_rate = error_rate
        self.size = self._get_size(capacity, error_rate)
        self.num_hashes = self._get_num_hashes(self.size, capacity)
        self.counts = [0] * self.size

    def _hashes(self, item):
        """Generate k hash values for the item."""
        hashes = []
        hash1 = int(hashlib.md5(item.encode('utf-8')).hexdigest(), 16)
        hash2 = int(hashlib.sha1(item.encode('utf-8')).hexdigest(), 16)
        for i in range(self.num_hashes):
            combined_hash = (hash1 + i * hash2) % self.size
            hashes.append(combined_hash)
        return hashes

    def _get_size(self, capacity, error_rate):
        """Calculate the size of the bit array (m)"""
        m = -capacity * math.log(error_rate) / (math.log(2) ** 2)
        return int(m)

    def _get_num_hashes(self, size, capacity):
        """Calculate the number of hash functions (k)"""
        k = (size / capacity) * math.log(2)
        return int(k)

    def add(self, item):
        """Add an item to the filter"""
        for hash_value in self._hashes(item):
            self.counts[hash_value] += 1

    def remove(self, item):
        """Remove an item from the filter"""
        if item in self:  # Check if item exists before removing
            for hash_value in self._hashes(item):
                self.counts[hash_value] -= 1

    def __contains__(self, item):
        """Check if an item is in the filter"""
        return all(self.counts[hash_value] > 0 for hash_value in self._hashes(item))

    def is_empty(self):
        """Check if the filter has no active elements"""
        return all(count == 0 for count in self.counts)


class DetachedCountingBloomFilterArray:
    def __init__(self, capacity_per_filter, error_rate=0.001):
        self.capacity_per_filter = capacity_per_filter
        self.error_rate = error_rate
        self.sub_filters = []

    def add_filter(self):
        """Add a new sub-filter"""
        new_filter = CountingBloomFilter(self.capacity_per_filter, self.error_rate)
        self.sub_filters.append(new_filter)

    def add(self, item, filter_index):
        """Add an item to a specific sub-filter"""
        if filter_index < len(self.sub_filters):
            self.sub_filters[filter_index].add(item)
        else:
            raise IndexError("Filter index out of bounds")

    def remove(self, item, filter_index):
        """Remove an item from a specific sub-filter"""
        if filter_index < len(self.sub_filters):
            self.sub_filters[filter_index].remove(item)
        else:
            raise IndexError("Filter index out of bounds")

    def query(self, item):
        """Query all sub-filters to check if an item is present"""
        return any(item in sub_filter for sub_filter in self.sub_filters)

    def detach_filter(self, filter_index):
        """Detach and remove a sub-filter when it is no longer needed"""
        if filter_index < len(self.sub_filters):
            del self.sub_filters[filter_index]
        else:
            raise IndexError("Filter index out of bounds")

    def garbage_collect(self):
        """Garbage collect empty sub-filters"""
        self.sub_filters = [f for f in self.sub_filters if not f.is_empty()]


# Example Usage
if __name__ == "__main__":
    dcbfa = DetachedCountingBloomFilterArray(capacity_per_filter=1000, error_rate=0.01)
    
    # Adding sub-filters (detached Bloom filters)
    dcbfa.add_filter()  # Sub-filter 0
    dcbfa.add_filter()  # Sub-filter 1

    # Adding items to specific sub-filters
    dcbfa.add("item1", 0)
    dcbfa.add("item2", 0)
    dcbfa.add("item3", 1)
    
    # Querying items in any sub-filter
    print("item1 in filters:", dcbfa.query("item1"))  # True
    print("item3 in filters:", dcbfa.query("item3"))  # True
    print("item4 in filters:", dcbfa.query("item4"))  # False
    
    # Remove an item
    dcbfa.remove("item1", 0)
    print("item1 in filters after removal:", dcbfa.query("item1"))  # False

    # Detach a sub-filter
    dcbfa.detach_filter(0)
    print("item2 in filters after detaching filter 0:", dcbfa.query("item2"))  # False
    
    # Garbage collect empty filters
    dcbfa.garbage_collect()
