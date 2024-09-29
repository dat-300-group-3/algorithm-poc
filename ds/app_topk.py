from app_default import Operation, ClickEvent
from input.input_reader import AppRedisClient
from datetime import datetime


class TopKSlidingWindow(Operation):
    def __init__(self, window_size, k):
        self.redis_client = AppRedisClient().get_connection()
        self.window_size = window_size  # in seconds
        self.k = k
        self.key = "click:topk"
        self.redis_client.execute_command(
            "TOPK.RESERVE", self.key, self.k, 2000, 7, 0.925
        )

    def add_element(self, element: ClickEvent):
        time_obj = datetime.strptime(element.click_time, "%Y-%m-%d %H:%M:%S")
        event_time = int(time_obj.timestamp())
        ckey = f"{element.ip}-{element.device}-{element.channel}"
        sub_key = f"{event_time}-{ckey}"

        self.redis_client.execute_command("TOPK.ADD", self.key, ckey)
        self.redis_client.zadd("timeseries", {sub_key: event_time})

        #threshold check
        #topk_items = self.redis_client.execute_command("TOPK.LIST", self.key)
        #topk_scores = self.redis_client.execute_command(
        #    "TOPK.COUNT", self.key, *topk_items
        #)
        current_element_count = self.redis_client.execute_command(
            "TOPK.COUNT", self.key, ckey
        )

        if current_element_count[0] > 10:
            print("fraud detected")
        #topk_with_scores = dict(zip(topk_items, topk_scores))
        #for value, count in topk_with_scores.items():
        #    print(value, "count-", count)


        # Remove elements that fall out of the sliding window
        #self.remove_element(event_time)

    def remove_element(self, event_time):
        cutoff_time = event_time - self.window_size
        elements_to_remove = self.redis_client.zrangebyscore(
            "timeseries", 0, cutoff_time
        )
        for element in elements_to_remove:
            self.redis_client.execute_command("TOPK.REMOVE", self.key, element)
            self.redis_client.zrem("timeseries", element)