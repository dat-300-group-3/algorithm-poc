import matplotlib.pyplot as plt
from app_default import AppRedisClient


class YourClass:
    def __init__(self) -> None:
        self.redis_client = AppRedisClient().get_connection()

    def plot_cms_data(self, period):
        # Retrieve all unique elements from the Redis Set
        values = self.redis_client.smembers(f"ulookup:{period}")

        # Initialize lists to store the elements and their corresponding counts
        elements = []
        counts = []

        # Iterate through each element, query its count from the Count-Min Sketch
        for it in values:
            # Convert byte response to string (since smembers returns bytes)
            element = it  # Redis typically returns bytes, so decode it

            # Query the count for each element
            val = self.redis_client.cms().query(f"cms:{period}", element)

            # Store the element and its count in respective lists
            elements.append(element)
            counts.append(val[0])  # `val` is returned as a list, so access the count with val[0]

        # Plot the elements vs. counts
        self.create_plot(elements, counts)

    def create_plot(self, elements, counts):
        # Create a bar plot
        plt.figure(figsize=(10, 6))
        plt.bar(elements, counts, color='skyblue')

        # Set the title and labels
        plt.title('Count-Min Sketch Data Plot')
        plt.xlabel('Elements')
        plt.ylabel('Counts')

        # Rotate X-axis labels for better readability (optional)
        plt.xticks(rotation=45, ha='right')

        # Show the plot
        plt.tight_layout()
        plt.show()

# Usage
your_instance = YourClass()

# Plot the entire dataset at once without chunking
your_instance.plot_cms_data('2017-11-06:15')
