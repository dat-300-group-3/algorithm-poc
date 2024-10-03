import csv
import redis
import math
import multiprocessing as mp

# Define constants
CSV_FILE_PATH = './data/test_split.csv'
TOPK_KEY = 'ip_topk'

CHUNK_SIZE = 10000  # Number of rows per chunk for multiprocessing
PROCESS_COUNT = mp.cpu_count()  # Number of processes

# Function to reserve a Top-K sketch in Redis
def setup_topk(redis_conn, k=5000):
    width = int(k * math.log(k))
    depth = int(math.log(k))
    redis_conn.topk().reserve(TOPK_KEY, k, width, depth, 0.9)

# Function to process each chunk of the CSV file and add IPs to Top-K sketch
def process_chunk(start, end):
    # Create a new Redis connection for each process
    redis_conn = redis.Redis()

    with open(CSV_FILE_PATH, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for i, row in enumerate(csv_reader):
            if i < start:
                continue
            if i >= end:
                break
            ip = row['ip']
            redis_conn.topk().add(TOPK_KEY, ip)
            print(f"Added IP {ip} to the Top-K sketch from rows {start} to {end}")

# Function to manage multiprocessing
def multiprocessing_topk():
    # Create Redis connection to set up the Top-K sketch (only needs to be done once)
    redis_conn = redis.Redis()

    # Set up the Top-K sketch if not already defined
    setup_topk(redis_conn)

    # Determine the total number of rows in the CSV
    with open(CSV_FILE_PATH, mode='r') as file:
        csv_reader = csv.DictReader(file)
        total_rows = sum(1 for _ in csv_reader)

    # Calculate chunk ranges for each process
    chunk_ranges = [(i, min(i + CHUNK_SIZE, total_rows)) for i in range(0, total_rows, CHUNK_SIZE)]

    # Use multiprocessing pool to handle multiple chunks concurrently
    with mp.Pool(PROCESS_COUNT) as pool:
        # Each worker will handle a chunk of the file
        pool.starmap(process_chunk, chunk_ranges)

    print("All IPs have been added to the Top-K sketch!")

if __name__ == '__main__':
    multiprocessing_topk()
