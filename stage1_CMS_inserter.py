import redis
import pandas as pd


# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
CSV_FILE = './data/24h_raw.csv'

width = 1000000
depth = 10

CMS_KEY = f'cms:click_data_w{width}_{depth}d'


# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Create Count-Min Sketch if not exists (width=2000, depth=5)
r.execute_command('CMS.INITBYDIM', CMS_KEY, width, depth)

# Set to store unique keys
unique_keys = set()
verified_users_keys = set()

# Read CSV file
chunk_size = 100000  # Process in chunks for large files
count = 0
for chunk in pd.read_csv(CSV_FILE, chunksize=chunk_size):
    # Prepare pipeline for batch processing
    pipeline = r.pipeline()
    
    for _, row in chunk.iterrows():
        ip = str(row['ip'])
        app = str(row['app'])
        device = str(row['device'])
        os = str(row['os'])
        channel = str(row['channel'])
        
        # Use a unique key for each combination (you can modify this to include more fields if needed)
        key = f"{ip}-{app}-{device}-{os}-{channel}"
        
        # Update Count-Min Sketch
        pipeline.execute_command('CMS.INCRBY', CMS_KEY, key, 1)
        
        # Add key to the set of unique keys
        unique_keys.add(key)
        count += 1

        if row['is_attributed'] == 1:
            verified_users_keys.add(key)

    # Execute the pipeline
    print(f"Processed {count} rows")
    pipeline.execute()

print("Count-Min Sketch populated successfully!")

# Remove identifiers present in the verified_users_keys
for keys in verified_users_keys:
    unique_keys.discard(keys)

# Save unique keys to CSV files
unique_keys_df = pd.DataFrame(list(unique_keys), columns=['unique_key'])
unique_keys_df.to_csv('all_keys.csv', index=False)

verified_users_keys_df = pd.DataFrame(list(verified_users_keys), columns=['verified_user_key'])
verified_users_keys_df.to_csv('verified_keys.csv', index=False)
