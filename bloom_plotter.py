import redis
import matplotlib.pyplot as plt
from collections import defaultdict

def get_duplicate_keys_and_values(redis_host='localhost', redis_port=6379):
    # Connect to the Redis server
    r = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    # Define the key pattern
    key_pattern = 'duplicates:*'

    try:
        # Get all keys matching the pattern
        keys = r.keys(key_pattern)

        # Dictionary to hold data grouped by date
        day_aggregated_data = defaultdict(lambda: defaultdict(int))

        if keys:
            for key in keys:
                # Extract date from the key (e.g., from 'duplicates:2017-11-06:15' to '2017-11-06')
                date_label = key.split(':')[1]

                # Get the hash values for each key
                values = r.hgetall(key)
                for k, v in values.items():
                    #if int(v) > 5:
                        # Aggregate the counts by date and combination (k)
                        day_aggregated_data[date_label][k] += int(v)

            # Create a plot for each day
            for date, k_v_dict in day_aggregated_data.items():
                # Extract keys (combinations) and summed values for plotting
                k_values = list(k_v_dict.keys())
                v_values = list(k_v_dict.values())

                # Filter out v_values that are 1, and update corresponding k_values
                #k_values = list(k_v_dict.keys())
                #v_values = list(k_v_dict.values())
                k_values, v_values = zip(*[(k, v) for k, v in zip(k_values, v_values) if v != 1])

                # Create a separate plot for each day
                plt.figure(figsize=(18, 10))
                plt.bar(k_values, v_values, color='blue')
                plt.xlabel('Combination (k)')
                plt.ylabel('Sum of Duplicate Counts (v)')
                plt.title(f'Duplicate Counts for {date}')
                plt.xticks(rotation=45)  # Rotate x labels for better readability
                plt.grid(axis='y')  # Add a grid for better visualization
                plt.tight_layout()  # Adjust layout to prevent clipping of labels

                # Show the plot
                plt.show()

        else:
            print(f"No keys found matching the pattern '{key_pattern}'.")

    except redis.exceptions.RedisError as e:
        print(f"Error fetching data from Redis: {e}")

if __name__ == "__main__":
    get_duplicate_keys_and_values()
