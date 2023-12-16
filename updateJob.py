import redis
import json
import datetime
import config
import logging

from database import r
from activeMinig import mining_status
from requestJob import request_job


class MessageType:
    DOWNLOADFILE = "downloadFile"
    REQUESTJOB = "requestJob"


def update_jobs(new_wallet_address):
    try:
        current_time = datetime.datetime.now()
        ten_minutes_ago = current_time - datetime.timedelta(minutes=10)

        # Fetch the value of 'active_mining'
        active_mining_status = r.get("mining_status")
        print("active_mining_status", active_mining_status)
        if active_mining_status in [None, False, "False"]:
            print("Error: 'mining_status' not found in Redis.")
            request_job(config.WALLET_ADDRESS, MessageType.REQUESTJOB)
            logging.info("New Job was requested from the Inode")
            return None, None

        active_mining_value = r.get("active_mining")
        if not active_mining_value:
            print("Error: 'active_mining' not found in Redis.")
            return None, None

        # Retrieve the specific job using the 'active_mining_value'
        file_hashes = r.hgetall(active_mining_value)
        if not file_hashes:
            print(f"Error: No job found with ID {active_mining_value}.")
            return None, None

        for file_hash, json_data in file_hashes.items():
            try:
                data = json.loads(json_data)
            except json.JSONDecodeError:
                # Handle JSON decode error
                continue  # Skip to the next item

            # Check if 'gradient' is missing
            if "gradient" not in data or not data["gradient"]:
                updated = False

                # Handling for last_active field if it's not in expected format
                try:
                    last_active_time = datetime.datetime.fromisoformat(
                        data.get("last_active")
                    )
                except TypeError:
                    last_active_time = (
                        datetime.datetime.min
                    )  # Assign some default value

                # If 'wallet' exists and 'last_active' is older than 10 minutes
                if "wallet" in data and last_active_time < ten_minutes_ago:
                    data["wallet"] = new_wallet_address
                    data["last_active"] = current_time.isoformat()
                    data["downloaded"] = "1"
                    updated = True

                # If 'wallet' does not exist
                elif "wallet" not in data:
                    data["wallet"] = new_wallet_address
                    data["last_active"] = current_time.isoformat()
                    data["downloaded"] = "1"
                    updated = True

                # If data was updated, save it back to Redis
                if updated:
                    json_data = json.dumps(data)
                    r.hset(active_mining_value, file_hash, json_data)
                    return json.dumps(
                        {
                            "file_hash": file_hash,
                            "url": data["url"],
                            "active_mining_value": active_mining_value,
                            "message_type": MessageType.DOWNLOADFILE,
                        }
                    )
                else:
                    print("All jobs were processed successfully")
                    mining_status(False)

        return None, None  # Return None if no updates were made

    except redis.RedisError as e:
        # Handle Redis-specific errors
        print(f"Redis error: {e}")
        return None, None
    except Exception as e:
        # Handle other generic errors
        print(f"An error occurred: {e}")
        return None, None
