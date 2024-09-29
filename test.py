import redis
import time
import csv
from datetime import date


# Connect to Redis
r = redis.Redis()

def is_allowed(row):
    today = date.today()

    current_time = int(time.time())
    print("Date:",today)
    key = f"rate_limit:{today}"  # Generate a Redis key
    
    pipe = r.pipeline()
    
    #window size is one hour
    pipe.zremrangebyscore(key, 0, current_time - 3600)
    pipe.zadd(key, {"%d-%s-%s-%s" %(current_time, row['ip'], row['device'], row['channel']): current_time})
    pipe.zcard(key)
    #expire in one minute
    pipe.expire(key, 60)
    
    # Execute the pipeline
    action_countv = pipe.execute()
    print(action_countv)
    action_count = action_countv[2]

    limit = 5

    # Check if the user has exceeded the limit
    if action_count <= limit:
        return True  # Action is allowed
    else:
        return False  # Rate limit exceeded

with open('data/sorted_file.csv', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if is_allowed(row):
                     print("Action allowed")
                else:
                    print("Rate limit exceeded")
