import pandas as pd
from app_common import AppRedisClient, AppUtils, Writer


class StageOne:
    def __init__(self, redis_client: AppRedisClient, cms_key, csv_file, chunk_size):
        self.redis_conn = redis_client.get_connection()
        self.cms_key = cms_key
        self.csv_file = csv_file
        self.chunk_size = chunk_size
        self.potential_fraud_users = set()
        self.genuine_users = set()

    def start_cms_insert(self):
        """Read CSV and process data in chunks."""

        for chunk in pd.read_csv(self.csv_file, chunksize=self.chunk_size):
            with self.redis_conn.pipeline() as pipeline:
                for _, row in chunk.iterrows():
                    key = AppUtils.get_ip_app_device_os_channel_key(row)

                    # add key to cms
                    pipeline.cms().incrby(self.cms_key, [key], [1])

                    if row["is_attributed"] == 1:
                        self.genuine_users.add(key)
                    else:
                        self.potential_fraud_users.add(key)

                print(f"Processed {chunk.shape[0]} rows")
                pipeline.execute()

        print("Count-Min Sketch populated successfully!")

    def clean_up(self):
        self.potential_fraudulent_unique_users.clear()
        self.genuine_users.clear()


if __name__ == "__main__":
    redis = AppRedisClient()
    width = 10000
    depth = 20
    batch_size = 820000
    CMS_KEY = f"cms:click_data_w{width}_{depth}d"
    redis.initialize_cms(CMS_KEY, width, depth)

    stage_one = StageOne(redis, CMS_KEY, "data/sorted_file.csv", batch_size)
    stage_one.start_cms_insert()

    csv_writer = Writer()
    csv_writer.save_to_csv(
        "data/potential_fraud_users.csv",
        stage_one.potential_fraud_users,
        ["potential_fraud_user_keys"],
    )
    csv_writer.save_to_csv(
        "data/genuine_users.csv", stage_one.genuine_users, ["genuine_user_keys"]
    )
