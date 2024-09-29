import redis
from threading import Lock


class AppRedisClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                #double check on instance
                if not cls._instance:
                    cls._instance = super(AppRedisClient, cls).__new__(cls)
                    cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        self.connection = redis.Redis(
            host="localhost",  
            port=6379, 
            db=0,  # Redis database number (default is 0)
            decode_responses=True,  # Enable human-readable responses
        )

    def get_connection(self):
        return self.connection
