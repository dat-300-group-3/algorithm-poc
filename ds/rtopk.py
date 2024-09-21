import redis

class RTopK:
    def __init__(self) -> None:
        r = redis.Redis(decode_responses=True)
        #assuming data is already available in redis
        self.topk = r.topk()

    def get_top_k(self) -> list:
        return self.topk.list("topk:ip-device-channel")
    
    def print_count_top_k(self, top_items):
        for item in top_items:
            count = self.topk.count("topk:ip-device-channel", item)
            print(f"IP: {item}, Count: {count}")