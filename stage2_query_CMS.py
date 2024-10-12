import redis
import pandas as pd

# Connect to Redis
r = redis.Redis()


width = 1000000
depth = 10

CMS_KEY = f'cms:click_data_w{width}_{depth}d'

file_name = f'w{width}_{depth}d.csv'

#CMS_KEY = 'cms:click_data_w59M_5d'

# Load data from CSV files
print("Loading data from CSV files...")
df_all_keys = pd.read_csv('unique_keys.csv')
df_verified_keys = pd.read_csv('verified_keys.csv')
print("Data loaded successfully!")


verified_users_keys = df_verified_keys['verified_user_key'].values

verified_users_set = set(verified_users_keys)
df_all_keys = df_all_keys[~df_all_keys['unique_key'].isin(verified_users_set)]

filtered_unique_keys = df_all_keys['unique_key'].values



BATCH_SIZE = 100000

# Use a Redis pipeline to query the Count-Min Sketch for unique keys
print("Preparing to query all keys...")
all_keys = []
batch_keys = []

with r.pipeline() as pipe:
    for idx, key in enumerate(filtered_unique_keys):
        pipe.cms().query(CMS_KEY, key)
        batch_keys.append(key)

        # Execute the pipeline in batches
        if (idx + 1) % BATCH_SIZE == 0:
            all_keys.extend(pipe.execute())
            pipe.reset()
            batch_keys = []
            print(f"Processed {idx + 1} unique keys")
            

    # Execute any remaining keys in the pipeline
    if batch_keys:
        all_keys.extend(pipe.execute())

print("All keys queried successfully!")

# Decode the unique keys and counts into the desired format
formatted_output_unique = []
print("Formatting unique keys...")
for key, count in zip(filtered_unique_keys, all_keys):
    ip, app, device, os, channel = key.split('-')
    identifier = key
    frequency = count[0] if count else 0
    formatted_output_unique.append(f"{identifier},{ip},{app},{device},{os},{channel},{frequency}")
print("Unique keys formatted successfully!")



# Use a Redis pipeline to query the Count-Min Sketch for verified users keys
print("Preparing to query verified users keys...")
verified_users_counts = []
batch_keys = []
with r.pipeline() as pipe:
    for idx, key in enumerate(verified_users_keys):
        pipe.cms().query(CMS_KEY, key)
        batch_keys.append(key)

        # Execute the pipeline in batches
        if (idx + 1) % BATCH_SIZE == 0:
            verified_users_counts.extend(pipe.execute())
            pipe.reset()
            batch_keys = []
            print(f"Processed {idx + 1} verified users keys")
    if batch_keys:
        verified_users_counts.extend(pipe.execute())
    
print("Verified users keys queried successfully!")

# Decode the verified users keys and counts into the desired format
formatted_output_verified = []
print("Formatting verified users keys...")
for key, count in zip(verified_users_keys, verified_users_counts):
    ip, app, device, os, channel = key.split('-')
    identifier = key
    frequency = count[0] if count else 0
    formatted_output_verified.append(f"{identifier},{ip},{app},{device},{os},{channel},{frequency}")
print("Verified users keys formatted successfully!")


# Save the formatted outputs to CSV files
print("Saving unique_keys_output to CSV files...")
with open(f'./data/all_rows_{file_name}', 'w') as f:
    f.write("identifier,ip,app,device,os,channel,frequency\n")
    for line in formatted_output_unique:
        f.write(f"{line}\n")
    print("unique_keys_output saved successfully!")

print("Saving verified_users_output to CSV files...")
with open(f'./data/verified_{file_name}', 'w') as f:
    f.write("identifier,ip,app,device,os,channel,frequency\n")
    for line in formatted_output_verified:
        f.write(f"{line}\n")
    print("verified_users_output saved successfully!")