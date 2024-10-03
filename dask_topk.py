import dask.dataframe as dd
import redis
import math
from datetime import datetime, timedelta
import pandas as pd

# Paths and Redis TopK settings
CSV_FILE_PATH = './data/train.csv'
TOPK_KEY = 'ip_5hours_dask'
TIME_LIMIT_MINUTES = 300

# Connect to Redis Stack
r = redis.Redis()

# Define a Top-K sketch if it hasn't been defined
k = 5000
width = int(k * math.log(k))
depth = int(math.log(k))
r.topk().reserve(TOPK_KEY, k, width, depth, 0.9)

# Helper function to process each partition
def process_partition(partition, watermark):
    # Redis connection inside Dask worker (ensure thread safety)
    r = redis.Redis()

    for idx, row in partition.iterrows():
        ip = row['ip']
        click_time_str = row['click_time']

        # Try to convert click_time to a datetime object and handle errors
        try:
            click_time = datetime.strptime(click_time_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Skipping row with invalid click_time: {click_time_str}")
            continue

        # If this is the first row in the partition, initialize the watermark
        if watermark is None:
            watermark = click_time
            print(f"Watermark set to: {watermark}")

        # Check if the current row's click_time exceeds the 5-hour window
        if click_time > watermark + timedelta(minutes=TIME_LIMIT_MINUTES):
            print(f"Stopping insertion. Exceeded 5-hour window from watermark: {watermark}")
            break

        # Add the IP to the Redis Top-K sketch
        r.topk().add(TOPK_KEY, ip)
        print(f"Added IP {ip} to the Top-K sketch at click_time {click_time_str}")
    
    return pd.Series([watermark])  # Return watermark for the next partition

# Read the CSV file using Dask
df = dd.read_csv(CSV_FILE_PATH, dtype={'attributed_time': 'object'})  # Specify the dtype if necessary

# Set initial watermark to None (to be initialized with the first partition)
initial_watermark = None

# Define the output metadata (Dask needs this to know the output structure)
meta = pd.Series([None], name='watermark')

# Process each partition and update the watermark
# `map_partitions` applies the function to each partition, passing the watermark
df.map_partitions(lambda partition: process_partition(partition, initial_watermark), meta=meta).compute()

print("Processing complete.")
