import redis

# Connect to the Redis server
r = redis.Redis(host='localhost', port=6379, db=0)

# Create a pipeline to batch the commands
pipe = r.pipeline()

# Insert data into TopK
pipe.execute_command('TOPK.ADD', 'mytopk', 'apple', 'banana', 'cherry')
pipe.execute_command('TOPK.ADD', 'mytopk', 'banana', 'apple')
pipe.execute_command('TOPK.ADD', 'mytopk', 'cherry', 'banana', 'banana')

# First, get the TopK list
pipe.execute_command('TOPK.LIST', 'mytopk')

# Execute the pipeline to get the TopK list
results = pipe.execute()

# Extract the list of top items from the results
topk_items = results[-1]  # The last command is the TOPK.LIST command

# Decode the byte strings into regular strings
topk_items = [item.decode('utf-8') for item in topk_items]

# Now create another pipeline to get the count of each item in the TopK list
pipe = r.pipeline()
print("2nd at",pipe)

# For each item in the TopK list, get its count
for item in topk_items:
    pipe.execute_command('TOPK.COUNT', 'mytopk', item)

# Execute the pipeline to get the counts
counts = pipe.execute()

# Combine the top items with their counts
topk_with_counts = dict(zip(topk_items, counts))

# Output the TopK items with their counts
print("TopK Items and Counts:")
for item, count in topk_with_counts.items():
    print(f"{item}: {count}")
