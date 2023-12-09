import requests
import json
import time
from database import r


def fetch_balance(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        data = response.json()
        print("Balance Found:", data)
        return int(data["bal"])  # Convert the balance to an integer
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    return 0


def setup_initial_data():
    # Storing each miner's data as a JSON string under a field in the 'miners_list' hash
    miners_data = {
        "A": {"balance": 0, "wallet_address": "Address_A"},
        "B": {"balance": 0, "wallet_address": "Address_B"},
        "C": {"balance": 0, "wallet_address": "Address_C"},
        "D": {"balance": 0, "wallet_address": "Address_D"},
    }
    for miner_id, data in miners_data.items():
        r.hset("miners_list", miner_id, json.dumps(data))

    r.set("last_transaction_id", 0)
    # Initialize an empty 'transactions_list' hash
    r.hset("transactions_list", "init", "{}")


setup_initial_data()


def get_last_recorded_balance():
    # Get the last recorded balance, defaulting to 0 if not set
    return int(r.get("last_recorded_balance") or 0)


def update_miner_balances(new_balance):
    last_balance = get_last_recorded_balance()
    new_tokens = new_balance - last_balance  # Calculate the new tokens received

    if new_tokens > 0:
        miner_ids = ["A", "B", "C", "D"]
        share = new_tokens // len(miner_ids)
        for miner_id in miner_ids:
            data = json.loads(r.hget("miners_list", miner_id))
            data["balance"] += share
            r.hset("miners_list", miner_id, json.dumps(data))

    # Update the last recorded balance
    r.set("last_recorded_balance", new_balance)


def record_transaction(new_balance):
    transaction_id = r.incr("last_transaction_id")
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    transaction_data = {"wallet_balance": new_balance, "timestamp": timestamp}
    r.hset("transactions_list", transaction_id, json.dumps(transaction_data))


def process_withdrawal(miner_id, amount):
    data = json.loads(r.hget("miners_list", miner_id))
    if data["balance"] >= amount:
        data["balance"] -= amount
        r.hset("miners_list", miner_id, json.dumps(data))
        # Record withdrawal
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        withdrawal_info = f"{miner_id}:{amount}:{timestamp}:Completed"
        r.lpush("withdrawals", withdrawal_info)
        return True
    return False


api_url = "http://127.0.0.1:5500/balance.json"

while True:
    balance = fetch_balance(api_url)
    if balance > 0:
        update_miner_balances(balance)
        record_transaction(balance)
    time.sleep(30)
