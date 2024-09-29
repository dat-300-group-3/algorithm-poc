import redis

# Connect to Redis (default settings: localhost, port 6379)
client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Add elements to a sorted set with scores
client.zadd("my_sorted_set", {"Alice": 10, "Bob": 20, "Charlie": 30, "Dave": 40})

# Print the sorted set before deletion
print("Before zremrangebyscore:", client.zrange("my_sorted_set", 0, -1, withscores=True))

# Remove members from the sorted set where scores are between 15 and 35 (inclusive)
client.zremrangebyscore("my_sorted_set", 15, 35)

# Print the sorted set after deletion
print("After zremrangebyscore:", client.zrange("my_sorted_set", 0, -1, withscores=True))
