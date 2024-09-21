import redis
import csv
from datetime import datetime
from redis.exceptions import ResponseError


class LoadData:
    def __init__(self) -> None:
        self.r = redis.Redis(decode_responses=True)

    def add_topk_data(self):
        #BF.RESERVE {key} {error_rate} {capacity} [EXPANSION expansion] [NONSCALING]
        topk = self.r.topk()
        #topk.reserve("topk:ip-device-os-channel-app", 10, 2000, 7, 0.925)
        topk.reserve("topk:ip-device-channel", 10, 2000, 7, 0.925)
        with open('data/train_sample.csv', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                #topk.add("topk:ip-device-os-channel-app",f"{row['ip']}-{row['device']}-{row['os']}-{row['channel']}-{row['app']}")
                topk.add("topk:ip-device-channel",f"{row['ip']}-{row['device']}-{row['channel']}")

    def add_bloom_filter_data(self):
        bloom = self.r.bf()
        #bloom.reserve("bloom:ip-device-os-channel-app", 0.0001, 100000)
        bloom.reserve("bloom:ip-device-channel", 0.0001, 100000)
        with open('data/train_sample.csv', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                #bloom.add("bloom:ip-device-os-channel-app",f"{row['ip']}-{row['device']}-{row['os']}-{row['channel']}-{row['app']}")
                bloom.add("bloom:ip-device-channel",f"{row['ip']}-{row['device']}-{row['channel']}")

    def add_as_time_series(self):
        ts = self.r.ts()
        with open('data/train_sample.csv', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                time_obj = datetime.strptime(row['click_time'], '%Y-%m-%d %H:%M:%S')
                epoch_millisec = int(time_obj.timestamp() * 1000)

                ip_key = f"{row['ip']}-{row['device']}-{row['channel']}"

                try:
                    ts.create(ip_key)
                except ResponseError as e:
                    pass
                try:
                    ts.add(ip_key, epoch_millisec, 1)
                except ResponseError as e:
                    if "DUPLICATE_POLICY is set to BLOCK mode" in str(e):
                        print("found a duplicate timestamp")
                
                
    
    def flush_all(self):
        self.r.flushdb()

load = LoadData()
#load.add_bloom_filter_data()
load.add_topk_data()
load.add_as_time_series()