from client.app_redis import AppRedisClient
from app_default import Reader, ClickEvent
import csv
from utils.app_constant import INPUT_CHANNEL, INPUT_FILE


class InputReader(Reader):

    def __init__(self) -> None:
        self.redis = AppRedisClient().get_connection()

    def publish_input(self):

        with open(INPUT_FILE, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            for i, row in enumerate(reader):
                # testing
                #if i == 1000:
                #    break

                self.redis.publish(
                    INPUT_CHANNEL,
                    ClickEvent(
                        row["ip"],
                        row["app"],
                        row["device"],
                        row["os"],
                        row["channel"],
                        row["click_time"],
                        row["attributed_time"],
                        row["is_attributed"],
                    ).to_json(),
                )
