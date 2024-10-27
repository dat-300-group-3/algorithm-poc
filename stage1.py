import redis
import pandas as pd
import os as osos

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

width = 1000000
depth = 10



# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Directory containing the 2-hour chunks
input_dir = 'input'
output_dir = 'identifiers'

osos.makedirs(output_dir, exist_ok=True)

# Iterate over all CSV files in the directory
for file_name in osos.listdir(input_dir):
    if file_name.endswith('.csv'):
        file_path = osos.path.join(input_dir, file_name)
        
        # Create Count-Min Sketch if not exists
        CMS_KEY = f'cms_1kk:{file_name}'
        r.execute_command('CMS.INITBYDIM', CMS_KEY, width, depth)
        
        
        # Set to store unique keys and verified users
        unique_keys = set()
        verified_users_keys = set()
        
        # Read CSV file
        chunk_size = 100000  # Process in chunks for large files
        count = 0
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Prepare pipeline for batch processing
            with r.pipeline() as pipeline:
            
                for _, row in chunk.iterrows():
                    ip = str(row['ip'])
                    app = str(row['app'])
                    device = str(row['device'])
                    os_field = str(row['os'])  # renamed 'os' to 'os_field' to avoid conflicts
                    channel = str(row['channel'])

                    # Use a unique key for each combination (you can modify this to include more fields if needed)
                    key = f"{ip}-{app}-{device}-{os_field}-{channel}"

                    # Update Count-Min Sketch
                    pipeline.execute_command('CMS.INCRBY', CMS_KEY, key, 1)

                    # Add key to the set of unique keys
                    unique_keys.add(key)
                    count += 1

                    if row['is_attributed'] == 1:
                        verified_users_keys.add(key)

                # Execute the pipeline
                print(f"Processed {count} rows from {file_name}")
                pipeline.execute()

        # Remove identifiers present in the verified_users_keys
        for key in verified_users_keys:
            unique_keys.discard(key)

        unique_output_file = osos.path.join(output_dir, f'users_{file_name}')
        verified_output_file = osos.path.join(output_dir, f'verified_users_{file_name}')

        
        # Save unique keys to CSV files
        unique_keys_df = pd.DataFrame(list(unique_keys), columns=['unique_key'])
        unique_keys_df.to_csv(unique_output_file, index=False)

        verified_users_keys_df = pd.DataFrame(list(verified_users_keys), columns=['verified_user_key'])
        verified_users_keys_df.to_csv(verified_output_file, index=False)

print("Count-Min Sketch populated successfully!")
