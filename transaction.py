import json
import random
import time
import threading


def update_transactions(file_path):
    senders = ["INODE"] * 5 + ["Bob", "Charlie", "Diana", "Eve"]
    receivers = ["MINER"] * 5 + ["Bob", "Charlie", "Diana", "Eve"]

    while True:
        with open(file_path, "r") as file:
            data = json.load(file)

        for transaction in data["transactions"]:
            transaction["sender"] = random.choice(senders)
            transaction["receiver"] = random.choice(receivers)

        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

        time.sleep(10)


def update_blockheight(file_path):
    while True:
        with open(file_path, "r") as file:
            data = json.load(file)

        data["blockheight"] = str(int(data["blockheight"]) + 1)

        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

        time.sleep(10)


# Running both functions in separate threads
if __name__ == "__main__":
    transaction_thread = threading.Thread(
        target=update_transactions, daemon=True, args=("transaction.json",)
    )
    blockheight_thread = threading.Thread(
        target=update_blockheight, daemon=True, args=("blockheight.json",)
    )

    transaction_thread.start()
    blockheight_thread.start()

    transaction_thread.join()
    blockheight_thread.join()
