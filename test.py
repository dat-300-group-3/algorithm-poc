import redis

# Connect to Redis
r = redis.Redis()

#Get the top 5 IPs
elements = r.topk().list('ip_device:24h', True)
paired_elements = list(zip(elements[::2], elements[1::2]))

# Print the top 5 IPs
for tuple in paired_elements:
    print(f"IP: {tuple[0]}, Count: {tuple[1]}")
