import dask.dataframe as dd
from collections import deque
import pandas as pd
from redis.exceptions import RedisError
from utils.app_constant import (
    BLOOM_FILTER_KEY,
    DUPLICATE_COUNT_KEY,
    HASHKEY_DUP_KEY,
    HASHKEY_LOOKUP_KEY,
    SAMPLE_FILE,
    TEST_FILE,
    TRAIN_FILE,
)
from app_default import Reader, Operation,AppRedisClient
from bucket_plotter import BucketStats


class DetectDuplicateWithBloomFilter(Reader):
    def __init__(self, window_size) -> None:
        self.df = dd.read_csv(
            SAMPLE_FILE,
            dtype={
                "ip": "int64",
                "app": "int64",
                "device": "int64",
                "os": "int64",
                "channel": "int64",
                "click_time": "object",
                "attributed_time": "object",
                "is_attributed": "int64",
            },
        )
        self.df["click_time"] = self.df["click_time"].map_partitions(self._date_parser)
        self.df["attributed_time"] = self.df["attributed_time"].map_partitions(
            self._date_parser
        )
        self.window_finder = deque()
        self.window_size = window_size


    def _date_parser(self, date_colum):
        return pd.to_datetime(date_colum, format="%Y-%m-%d %H:%M:%S", errors="coerce")

    def start_reader(self):
        # creat fraud detector object
        detector = DuplicateFinderBloom()

        for i, partition in enumerate(self.df.partitions):
            partition = partition.compute()
            print(f"processsing partition {i} of length {len(partition)}")
            print(type(i))
            if i == 1:
                break
            for _, row in partition.iterrows():

                # get the bucket based on the click time
                att_date = row["click_time"].date()
                hour = row["click_time"].hour
                bucket_start = (hour // self.window_size) * self.window_size
                period = f"{att_date}:{bucket_start:02d}"

                if self.window_finder.count(period) == 0:
                    self.window_finder.append(period)

                if len(self.window_finder) == 3:
                    k = self.window_finder.popleft()
                    BucketStats(0).create_plot(k)
                
                detector.do_operation(
                    row["ip"],
                    row["app"],
                    row["device"],
                    row["os"],
                    row["channel"],
                    row["click_time"],
                    row["attributed_time"],
                    row["is_attributed"],
                    period
                )

    def cleanup(self):
        while self.window_finder:
            k = self.window_finder.popleft()
            BucketStats(0).create_plot(k)


class DetectDuplicateWithHashMap(Reader):
    def __init__(self) -> None:
        self.df = dd.read_csv(
            SAMPLE_FILE,
            dtype={
                "ip": "int64",
                "app": "int64",
                "device": "int64",
                "os": "int64",
                "channel": "int64",
                "click_time": "object",
                "attributed_time": "object",
                "is_attributed": "int64",
            },
        )
        self.df["click_time"] = self.df["click_time"].map_partitions(self._date_parser)
        self.df["attributed_time"] = self.df["attributed_time"].map_partitions(
            self._date_parser
        )

    def _date_parser(self, date_colum):
        return pd.to_datetime(date_colum, format="%Y-%m-%d %H:%M:%S", errors="coerce")

    def start_reader(self):
        # creat fraud detector object
        detector = DuplicateFinderHashMap()

        for i, partition in enumerate(self.df.partitions):
            partition = partition.compute()
            print(f"processsing partition {i} of length {len(partition)}")
            for _, row in partition.iterrows():
                detector.do_operation(
                    row["ip"],
                    row["app"],
                    row["device"],
                    row["os"],
                    row["channel"],
                    row["click_time"],
                    row["attributed_time"],
                    row["is_attributed"],
                )


class DuplicateFinderBloom(Operation):
    def __init__(self) -> None:
        self.redis_client = AppRedisClient().get_connection()
        self.bloom_filter = self.redis_client.bf()
        self.cm_sketch = self.redis_client.cms()

    def do_operation(
        self, ip, app, device, os, channel, click_time, attributed_time, is_attributed, period
    ):

        # unique user
        qkey = f"{ip}-{device}-{os}"

        if self.bloom_filter.exists(f"{BLOOM_FILTER_KEY}:{period}", qkey):
            self.redis_client.sadd(f"ulookup:{period}", qkey)
            try:
                self.cm_sketch.incrby(f"cms:{period}", [qkey], [1])
            except RedisError:
                self.cm_sketch.initbyprob(f"cms:{period}", 0.001, 0.002)
                self.cm_sketch.incrby(f"cms:{period}", [qkey], [1])
        else:
            self.bloom_filter.add(f"{BLOOM_FILTER_KEY}:{period}", qkey)


class DuplicateFinderHashMap(Operation):
    def __init__(self, window_size) -> None:
        self.redis_client = AppRedisClient().get_connection()
        self.window_size = window_size

    def do_operation(
        self, ip, app, device, os, channel, click_time, attributed_time, is_attributed
    ):
        # get the bucket based on the click time
        att_date = click_time.date()
        hour = click_time.hour
        bucket_start = (hour // self.window_size) * self.window_size
        period = f"{att_date}:{bucket_start:02d}"

        # unique user
        qkey = f"{ip}-{device}-{os}"

        self.redis_client.hincrby(f"{HASHKEY_DUP_KEY}:{period}", qkey, 1)


