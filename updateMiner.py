from database import r
import json
import logging


def update_miner(wallet, score, last_active_time):
    try:
        # Check if miners_list exists
        if not r.exists("miners_list"):
            r.hset(
                "miners_list",
                wallet,
                json.dumps(
                    {"balance": 0, "score": score, "last_active_time": last_active_time}
                ),
            )
            return True, "Miner data added successfully."
        else:
            # Check if the wallet already exists in miners_list
            miner_data = r.hget("miners_list", wallet)
            if miner_data:
                # Miner exists, update score and last_active_time
                miner_data = json.loads(miner_data)
                miner_data["score"] = str(
                    int(miner_data["score"]) + int(score)
                )  # Increment score
                miner_data["last_active_time"] = last_active_time
                r.hset("miners_list", wallet, json.dumps(miner_data))
                return True, "Miner updated successfully."
            else:
                # Miner does not exist, add to miners_list
                r.hset(
                    "miners_list",
                    wallet,
                    json.dumps(
                        {
                            "balance": 0,
                            "score": score,
                            "last_active_time": last_active_time,
                        }
                    ),
                )
                return True, "Miner score updated successfully."
    except json.JSONDecodeError:
        logging.error("Error in processing JSON data.")
        return False, "Failed to process JSON data."
    except ValueError as e:
        logging.error(f"Value error: {e}")
        return False, f"Value error: {e}"
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False, f"An unexpected error occurred: {e}"
