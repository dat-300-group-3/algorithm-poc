import datasketches as ds
import numpy as np
import pandas as pd

# import data
file_path = './data/original/test_split.csv'
data = pd.read_csv(file_path, nrows=1000)
print(data.head())
print("Data loaded")
print("________________________")

# Parameters
num_hashes = 4
num_buckets = 1024


# Create a new Count Min sketch
ds_sketch = ds.count_min_sketch(num_hashes, num_buckets)

# Update the sketch with the data
print("Updating sketch")
for index, row in data.iterrows():
    ds_sketch.update(row['ip'])
    

print("________________________")
# Query the sketch
print("Querying sketch")
query = 101399
result = ds_sketch.get_estimate(query)
print("Estimated count for query: ", query, " is: ", result)

# Heavy Hitters
print("Heavy Hitters")
total_weight = ds_sketch.total_weight
threshold = 0.1 * total_weight
