import pandas as pd
from app_common import AppRedisClient, Writer


class StageTwo:
    def __init__(self, redis_client: AppRedisClient, batch_size) -> None:
        self.redis_client = redis_client.get_connection()
        self.batch_size = batch_size

    def query_cms(self, cms_key, keys_to_query) -> pd.DataFrame:
        inter_result = list()
        final_result = list()
        is_batch = False

        with self.redis_client.pipeline() as pipe:
            for idx, key in enumerate(keys_to_query):
                is_batch = True
                pipe.cms().query(cms_key, key)

                # Execute the pipeline in batches
                if (idx + 1) % self.batch_size == 0:
                    inter_result.extend(pipe.execute())
                    pipe.reset()
                    is_batch = False
                    print(f"Processed {idx + 1} keys")

            # Execute any remaining keys in the pipeline
            if is_batch:
                inter_result.extend(pipe.execute())
                pipe.reset()

        for key, count in zip(keys_to_query, inter_result):
            ip, app, device, os, channel = key.split("-")
            identifier = key
            frequency = count[0] if count else 0
            final_result.append([identifier, ip, app, device, os, channel, frequency])

        inter_result.clear()
        # return a copy and clear the array ?
        return final_result


if __name__ == "__main__":
    redis = AppRedisClient()
    width = 10000
    depth = 20
    batch_size = 820000
    CMS_KEY = f"cms:click_data_w{width}_{depth}d"
    file_name = f"w{width}_{depth}d.csv"

    stage_two = StageTwo(redis, batch_size)
    csv_writer = Writer()

    # read csv file data
    df_potential_fraud_users = pd.read_csv("data/potential_fraud_users.csv")
    # query cms for count
    df_potential_fraud_users_count = stage_two.query_cms(
        CMS_KEY,
        df_potential_fraud_users["potential_fraud_user_keys"].values,
    )
    # write frequence to csv
    csv_writer.save_to_csv(
        "data/freq_potential_fraud_users.csv",
        df_potential_fraud_users_count,
        ["identifier", "ip", "app", "device", "os", "channel", "frequency"],
    )

    # read csv file data
    df_genuine_users = pd.read_csv("data/genuine_users.csv")
    # query cms for count
    df_genuine_users_count = stage_two.query_cms(
        CMS_KEY, df_genuine_users["genuine_user_keys"].values
    )
    # write frequence to csv
    csv_writer.save_to_csv(
        "data/freq_genuine_users.csv",
        df_genuine_users_count,
        ["identifier", "ip", "app", "device", "os", "channel", "frequency"],
    )
