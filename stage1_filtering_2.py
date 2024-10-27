import csv
import redis
import pandas as pd
import os as osos
import time

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

width = 100000
depth = 10
batch_size = 100000  # Define a batch size for pipelines

input_file = './input/TP_6.csv'
rules_file = './data/w100k/stage23_rules/rules_TP_5.csv'

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Directory containing the 2-hour chunks
input_dir = 'input'
output_dir = './identifiers/'

hmap_key = 'TP_6_latency100000'

# Read the rules file
data = {}
with open(rules_file, mode='r') as csv_file:
    t_start = time.perf_counter()
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Skip header
    for row in csv_reader:
        column, value, threshold = row[0], int(row[1]), float(row[2])
        key = f"{column}-{value}"
        data[key] = threshold

# Create Count-Min Sketch if not exists
CMS_KEY = f'cms_100:filter_test2'
#r.execute_command('DEL', CMS_KEY)
#r.execute_command('CMS.INITBYDIM', CMS_KEY, width, depth)

r.execute_command('DEL', hmap_key)
#r.execute_command('CMS.INITBYDIM', CMS_KEY, width, depth)

# Sets to store unique keys and verified users
unique_keys = set()
verified_users_keys = set()
suspicious_users = set()

# Read CSV file in chunks
chunk_size = 50000
count = 0

# Pipelines for batch operations
insert_pipeline = r.pipeline()
query_pipeline = r.pipeline()

# List to track the keys that were added to the query pipeline
query_keys_list = []

for chunk in pd.read_csv(input_file, chunksize=chunk_size):
    for _, row in chunk.iterrows():
        t1 = time.perf_counter()
        ip = str(row['ip'])
        app = str(row['app'])
        device = str(row['device'])
        os_field = str(row['os'])  # renamed 'os' to 'os_field' to avoid conflicts
        channel = str(row['channel'])

        # Use a unique key for each combination (you can modify this to include more fields if needed)
        key = f"{ip}-{app}-{device}-{os_field}-{channel}"

        # Add to insert pipeline
        #insert_pipeline.execute_command('CMS.INCRBY', CMS_KEY, key, 1)
        insert_pipeline.execute_command('HINCRBY', hmap_key, key, 1)
        
        # Add to query pipeline and track the key
        #query_pipeline.execute_command('CMS.QUERY', CMS_KEY, key)
        query_pipeline.execute_command('HGET', hmap_key, key)
        query_keys_list.append(key)

        # Execute pipelines when batch size is reached
        if (count + 1) % batch_size == 0:
            # Execute the insert pipeline
            insert_pipeline.execute()
            
            # Execute the query pipeline and process results
            results = query_pipeline.execute()
            
            for i, query_result in enumerate(results):
                key_count = query_result[0]
                key = query_keys_list[i]  # Get the corresponding key from the list

                # Determine if the key exceeds a threshold
                ip, app, device, os_field, channel = key.split('-')
                query_keys = [f"app-{app}", f"device-{device}", f"os-{os_field}", f"channel-{channel}"]

                for qk in query_keys:
                    if qk in data and key_count >= data[qk]:
                        suspicious_users.add(key)
                        t2 = time.perf_counter()
                        print(f"{t2 - t1}")

            # Clear the pipelines and the key list after execution
            insert_pipeline = r.pipeline()
            query_pipeline = r.pipeline()
            query_keys_list = []

        # Add key to the set of unique keys
        unique_keys.add(key)
        count += 1

        if row['is_attributed'] == 1:
            verified_users_keys.add(key)

# Execute any remaining commands in the pipelines
if insert_pipeline:
    insert_pipeline.execute()

if query_pipeline:
    results = query_pipeline.execute()
    for i, query_result in enumerate(results):
        key_count = query_result[0]
        key = query_keys_list[i]  # Get the corresponding key from the list

        ip, app, device, os_field, channel = key.split('-')
        query_keys = [f"app-{app}", f"device-{device}", f"os-{os_field}", f"channel-{channel}"]

        for qk in query_keys:
            if qk in data and key_count >= data[qk]:
                suspicious_users.add(key)
                t2 = time.perf_counter()
                print(f"{t2 - t1}")

# Remove identifiers present in the verified_users_keys
for key in verified_users_keys:
    unique_keys.discard(key)

t_end = time.perf_counter()

# Save the suspicious users to a CSV file
sus_users_keys_df = pd.DataFrame(list(suspicious_users), columns=['suspicious_user_key'])
sus_users_keys_df.to_csv(f'suspicious_users_TP6_CN.csv', index=False)

print(f'{t_end - t_start} seconds to process {count} rows')
