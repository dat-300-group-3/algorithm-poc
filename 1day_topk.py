import dask.dataframe as dd
import redis
import math


CSV_FILE_PATH = './data/24h_no_isAttributed.csv'
TOPK_KEY = 'ip:24h'

# Connect to Redis Stack
r = redis.Redis()

# Define a Top-K sketch if it hasn't been defined
# This line will be ignored if the Top-K sketch is already created
k = 5000
width = int(k * math.log(k))
depth = int(math.log(k))
r.topk().reserve(TOPK_KEY, k, width, depth, 0.9)


# Helper function to process each partition of the dataframe
def process_partition(partition):
    r = redis.Redis()  # Create a Redis connection for each partition (thread-safe)
    
    # Iterate over each row and concatenate 'ip' and 'device' columns
    for idx, row in partition.iterrows():
        #ip_device = str(row['ip']) + '-' + str(row['device'])
        ip_device = row['ip']
        
        # Add the concatenated value to the Top-K sketch
        r.topk().add(TOPK_KEY, ip_device)
        

# Read the CSV file using Dask
df = dd.read_csv(CSV_FILE_PATH, dtype={'attributed_time': 'object'})  # Adjust dtype if necessary

# Apply the function to process each partition
df.map_partitions(process_partition).compute()

print("Processing complete.")
