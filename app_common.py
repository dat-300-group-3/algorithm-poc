import redis
import pandas as pd
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
        self.__connection = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def get_connection(self):
        """return redis connection."""
        return self.__connection

    def initialize_cms(self, cms_key, cms_width, cms_depth):
        """Initialize the Count-Min Sketch in Redis."""
        # self.__connection.cms().initbyprob(cms_key, cms_width, cms_depth)
        # self.__connection.execute_command('CMS.INITBYDIM', cms_key, cms_width, cms_depth)
        self.__connection.cms().initbydim(cms_key, cms_width, cms_depth)


class AppUtils:

    @staticmethod
    def get_unique_key(row):
        """combination of all ip-app-device-os-channel"""
        ip = str(row["ip"])
        app = str(row["app"])
        device = str(row["device"])
        os = str(row["os"])
        channel = str(row["channel"])
        return f"{ip}-{app}-{device}-{os}-{channel}"

    @staticmethod
    def get_unique_user_key(row):
        """combination of ip-device-os"""
        ip = str(row["ip"])
        # app = str(row['app'])
        device = str(row["device"])
        os = str(row["os"])
        # channel = str(row['channel'])
        return f"{ip}-{device}-{os}"

    @staticmethod
    def get_user_app_key(row):
        """combination of ip-device-os-app"""
        ip = str(row["ip"])
        app = str(row["app"])
        device = str(row["device"])
        os = str(row["os"])
        # channel = str(row['channel'])
        return f"{ip}-{device}-{os}-{app}"

    @staticmethod
    def get_user_channel_key(row):
        """combination of ip-device-os-channel"""
        ip = str(row["ip"])
        # app = str(row['app'])
        device = str(row["device"])
        os = str(row["os"])
        channel = str(row["channel"])
        return f"{ip}-{device}-{os}-{channel}"


class Writer:
    def __init__(self):
        pass

    def save_to_csv(self, file_name: str, content: list, columns: list):
        """Save data to csv file"""
        content_df = pd.DataFrame(content, columns=columns)
        content_df.to_csv(file_name, index=False)
