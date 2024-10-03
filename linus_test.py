import csv
import redis
import math
import multiprocessing as mp

# Connect to Redis Stack
r = redis.Redis()

# Define a Top-K sketch if it hasn't been defined
# This line will be ignored if the Top-K sketch is already created
k = 10000
width = k*math.log(k)
depth = math.log(k)
r.topk().reserve('ip_topk_baseline', k, width, depth, 0.9)

# Open the CSV file
CSV_FILE_PATH = './data/train_sample.csv'
with open(CSV_FILE_PATH, mode='r') as file:
    csv_reader = csv.DictReader(file)

    # Set a limit for how many rows to add
    MAX_ROWS = 20000
    row_count = 0
    
    # Iterate over each row and add the IP to the Top-K sketch
    for row in csv_reader:
        if row_count >= MAX_ROWS:
            break
        ip = row['ip']
        r.topk().add('ip_topk_1', ip)
        print(f"Added IP {ip} to the Top-K sketch")
        row_count += 1

print("IPs have been added to the Top-K sketch!")
