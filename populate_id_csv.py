import pandas as pd

# Load the CSV file into a DataFrame
csv_file_path = './data/24h_processed_isattributed.csv'  # Update this with the path to your CSV file

unique_keys_path = 'unique_keys.csv'
verified_users_keys_path = 'verified_users_keys.csv'

unique_keys = set()
verified_users_keys = set()

# Read CSV file
chunk_size = 100000  # Process in chunks for large files
count = 0
for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
    # Prepare pipeline for batch processing
        
    for _, row in chunk.iterrows():
        ip = str(row['ip'])
        app = str(row['app'])
        device = str(row['device'])
        os = str(row['os'])
        channel = str(row['channel'])
        
        # Use a unique key for each combination (you can modify this to include more fields if needed)
        key = f"{ip}:{app}:{device}:{os}:{channel}"

        unique_keys.add(key)

        if row['is_attributed'] == 1:
            verified_users_keys.add(key)
        count += 1

pd.DataFrame(list(unique_keys), columns=['unique_key']).to_csv(unique_keys_path, index=False)
pd.DataFrame(list(verified_users_keys), columns=['verified_user_key']).to_csv(verified_users_keys_path, index=False)
