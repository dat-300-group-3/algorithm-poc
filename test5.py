from pybloom import BloomFilter
from utils import range_fn, is_string_io, running_python_3


f = BloomFilter(capacity=10000, error_rate=0.001)

for i in range_fn(0, 50):
    _ = f.add(i)

print(0 in f)
print(f.count)