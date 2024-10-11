from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
import json
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

@dataclass
class ClickEvent:
    ip: str
    app: str
    device: str
    os: str
    channel: str
    click_time: str
    attributed_time: str
    is_attributed: bool

    def to_json(self):
        return json.dumps(asdict(self), default=str)

    @staticmethod
    def from_json(json_data):
        data = json.loads(json_data)
        return ClickEvent(**data)


class AppException(Exception):
    pass


class Reader(ABC):
    @abstractmethod
    def start_reader(self):
        pass


class Operation(ABC):
    @abstractmethod
    def do_operation(
        self, ip, app, device, os, channel, click_time, attributed_time, is_attributed
    ):
        pass