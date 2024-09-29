import threading
from client.app_redis import AppRedisClient
from utils.app_constant import INPUT_CHANNEL
from input.input_reader import InputReader
from ds.app_topk import TopKSlidingWindow
from app_default import ClickEvent


def data_publisher():
    InputReader().publish_input()


def start_topk_process():
    topk = TopKSlidingWindow(3600, 5)
    pubsub = AppRedisClient().get_connection().pubsub()
    pubsub.subscribe(INPUT_CHANNEL)
    print(f"Subscribed to {INPUT_CHANNEL}. Waiting for messages...")
    for ind, message in enumerate(pubsub.listen()):
        if message["type"] == "message":
            click_event = ClickEvent.from_json(message["data"])
            topk.add_element(click_event)
            if ind == 100000:
                pubsub.close()
            


if __name__ == "__main__":
    t1 = threading.Thread(
        target=data_publisher,
    )
    t2 = threading.Thread(
        target=start_topk_process,
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("Done!")
