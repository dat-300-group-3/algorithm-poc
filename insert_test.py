import csv
import redis
from multiprocessing import Pool, cpu_count

# Function to insert a chunk of data into Redis
def insert_chunk_to_redis(chunk_data, chunk_idx):
    # Create a new Redis connection for each process
    r = redis.Redis()

    # Iterate through the rows of the chunk
    for idx, row in enumerate(chunk_data):
        # Create a unique key for each row, e.g., 'click:{index + offset}'
        key = f"click:{chunk_idx * len(chunk_data) + idx}"
        
        # Insert data as a hash into Redis
        r.hset(key, mapping={
            "ip": row[0],  # Assuming 'ip' is the first column
            "app": row[1],  # Assuming 'app' is the second column
            "device": row[2],
            "os": row[3],
            "channel": row[4],
            "click_time": row[5],
            "attributed_time": row[6] if row[6] else "",  # Handle missing values
            "is_attributed": row[7]
        })
    
    print(f"Inserted chunk {chunk_idx} into Redis.")

# Function to handle CSV reading and multiprocessing
def process_csv_multiprocessing(file_path, chunksize=10000):
    num_processes = 4

    # Create a pool of workers
    with Pool(processes=num_processes) as pool:
        chunk_data = []
        chunk_idx = 0

        # Open the CSV file and read it in chunks
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header if necessary
            
            for row in reader:
                chunk_data.append(row)

                # When chunk size is reached, submit the chunk to the pool
                if len(chunk_data) == chunksize:
                    pool.apply_async(insert_chunk_to_redis, args=(chunk_data, chunk_idx))
                    chunk_data = []  # Reset chunk data
                    chunk_idx += 1

            # Process the last chunk if it has remaining rows
            if chunk_data:
                pool.apply_async(insert_chunk_to_redis, args=(chunk_data, chunk_idx))

        # Close the pool and wait for all processes to finish
        pool.close()
        pool.join()

    print("All data inserted into Redis successfully.")

# Example usage
if __name__ == '__main__':
    process_csv_multiprocessing('./data/train.csv', chunksize=10000)
