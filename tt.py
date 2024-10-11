import matplotlib.pyplot as plt
from app_default import AppRedisClient


class YourClass:
    def __init__(self) -> None:
        self.redis_client = AppRedisClient().get_connection()

    def query_cms_in_chunks(self, period, chunk_size=10000):
        # Retrieve all unique elements from the Redis Set using an iterator
        all_values = self.redis_client.sscan_iter(f"ulookup:{period}")

        chunk = []
        for idx, it in enumerate(all_values):
            # Decode the element (since Redis returns bytes)
            element = it

            # Query the count for this element in CMS
            val = self.redis_client.cms().query(f"cms:{period}", element)

            # Store the (element, count) in the chunk
            chunk.append((element, val[0]))  # Store tuple of element and count

            # If we have reached the chunk size, plot the current chunk
            if (idx + 1) % chunk_size == 0:
                print(f"Plotting chunk {idx + 1}")
                self.create_plot(chunk)
                chunk.clear()  # Clear the chunk after processing

        # Plot any remaining elements in the last chunk
        if chunk:
            print(f"Plotting the last chunk (size: {len(chunk)})")
            self.create_plot(chunk)

    def create_plot(self, data_chunk):
        elements, counts = zip(*data_chunk)  # Unpack the chunk into two lists

        # Create a bar plot
        plt.figure(figsize=(10, 6))
        plt.bar(elements, counts, color='skyblue')

        # Set the title and labels
        plt.title('Count-Min Sketch Data Plot')
        plt.xlabel('Elements')
        plt.ylabel('Counts')

        # Rotate X-axis labels for better readability
        plt.xticks(rotation=45, ha='right')

        # Show the plot
        plt.tight_layout()
        plt.show()

# Usage
your_instance = YourClass()

# Process and plot data in chunks of 10,000 items
your_instance.query_cms_in_chunks('2017-11-06:15', chunk_size=10000)
