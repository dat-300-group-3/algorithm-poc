import redis

class RBloomF:
    def __init__(self) -> None:
        r = redis.Redis(decode_responses=True)
        #assuming data is already available in redis
        self.bloom = r.bf()

    def is_exists(self, item) -> bool:
        return  self.bloom.exists("bloom:ip-device-os-channel-app", item)