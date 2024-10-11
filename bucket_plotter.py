import matplotlib.pyplot as plt
from app_default import AppRedisClient


class BucketStats:
    def __init__(self, threshold) -> None:
        self.redis_client = AppRedisClient().get_connection()
        self.threshold = threshold
    
    def create_plot(self, period):
        values = self.redis_client.smembers(f"ulookup:{period}")
        elements = []
        counts = []
        for it in values:
            val = self.redis_client.cms().query(f"cms:{period}", it)
            elements.append(it)
            counts.append(val[0])

        #values = {k: int(v) for k, v in values.items() if int(v) != self.threshold}
        #k_values, v_values = values.keys(), values.values()

        plt.figure(figsize=(10, 6))
        plt.bar(elements, counts, color='blue')
        plt.xlabel('Keys')
        plt.ylabel('Values')
        plt.title(f'Plot for Redis Key: {period}')
        plt.xticks(rotation=45)
        plt.grid(axis='y')

        plot_filename = f'data/images/{period}_plot.png'
        
        plt.tight_layout()
        plt.savefig(plot_filename)
        plt.close()  

        self.redis_client.delete(f"ulookup:{period}")
        #how to remove bloom filter key ?
