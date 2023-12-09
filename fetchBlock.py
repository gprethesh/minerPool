import requests
import time
from database import r
import json
from leveldatabase import store_in_leveldb, retrieve_from_leveldb


def fetch_block(api_url):
    # Existing fetch_block function
    try:
        print("Fetching block")
        response = requests.get(api_url)
        response.raise_for_status()  # Raises an exception for HTTP errors
        data = response.json()
        return data
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    return None


def fetch_block_height(api_url):
    try:
        print("fetching block height")
        response = requests.get(api_url)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        data = response.json()
        return int(data["blockheight"])  # Convert the balance to an integer
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    return 0


def update_miner_balances(amount, blockheight):
    miner_updates = {}
    try:
        # Fetch miners list from Redis
        miners_data = r.hgetall("miners_list")

        # Initialize filtered miners and total score
        filtered_miners = {}
        total_score = 0

        # Process each miner and handle individual data errors
        for miner, data in miners_data.items():
            try:
                miner_data = json.loads(data)
                score = int(miner_data["score"])
                if score > 0:
                    filtered_miners[miner] = miner_data
                    total_score += score
            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error processing miner {miner}: {e}")

        # Prevent division by zero
        if total_score == 0:
            raise ValueError("No scores were computed")

        # Update balance and reset score for each valid miner
        for miner, data in filtered_miners.items():
            try:
                previous_balance = data["balance"]
                score = int(data["score"])
                # Calculate the miner's share of the total amount
                miner_share = (score / total_score) * amount
                # Update balance and reset score
                data["balance"] += miner_share
                data["score"] = "0"
                # Save updated data back to Redis
                r.hset("miners_list", miner, json.dumps(data))
                miner_updates[miner] = {
                    "previous_balance": previous_balance,
                    "score": score,
                    "added_amount": miner_share,
                    "current_balance": data["balance"],
                }
            except Exception as e:
                print(f"Error updating miner {miner}: {e}")

        store_in_leveldb(blockheight, miner_updates)

        retrieve_from_leveldb(blockheight)

        print("Balances updated and scores reset.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def process_transactions(api_url):
    print(f"Processing transactions from {api_url}")
    try:
        block_height = fetch_block_height("http://127.0.0.1:5500/blockheight.json")
        print("block_height", block_height)
        block_data = fetch_block(api_url)
        # print("block_data", block_data)
        if block_data is None:
            raise ValueError("No block data retrieved.")

        # Filter transactions where sender is 'INODE' and receiver is 'MINER'
        filtered_transactions = [
            tx
            for tx in block_data.get("transactions", [])
            if tx.get("sender") == "INODE" and tx.get("receiver") == "MINER"
        ]

        # Process each filtered transaction
        for tx in filtered_transactions:
            try:
                amount = tx.get("amount")
                if amount is None:
                    raise ValueError("Transaction amount is missing.")
                print(f"Transaction amount: {amount}")
                update_miner_balances(amount, block_height)
            except ValueError as e:
                print(f"Error in transaction processing: {e}")

    except ValueError as e:
        print(f"Error fetching block data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
