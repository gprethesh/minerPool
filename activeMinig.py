import redis
import json
import datetime

# Connect to Redis
from database import r


def active_mining(value):
    print("Active mining called")
    try:
        # Store the given value under the key 'active_mining'
        r.set("active_mining", value)
        return True  # Return True to indicate success

    except redis.RedisError as e:
        # Handle Redis-specific errors
        print(f"Redis error: {e}")
        return False  # Return False to indicate failure
    except Exception as e:
        # Handle other generic errors
        print(f"An unexpected error occurred: {e}")
        return False  # Return False to indicate failure


def mining_status(value):
    print("mining_status called")
    try:
        # Store the given value under the key 'mining_status'
        r.set("mining_status", str(value))
        return True  # Return True to indicate success

    except redis.RedisError as e:
        # Handle Redis-specific errors
        print(f"Redis error: {e}")
        return False  # Return False to indicate failure
    except Exception as e:
        # Handle other generic errors
        print(f"An unexpected error occurred: {e}")
        return False  # Return False to indicate failure
