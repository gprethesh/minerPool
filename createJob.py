import redis
import json
import datetime
from database import r

# Connect to Redis


def create_job(job_id, file_hashes):
    try:
        print("Creating job")
        for file_hash, data in file_hashes.items():
            try:
                # Serialize the data to a JSON string
                json_data = json.dumps(data)
                # Store the JSON string under the job_id hash with the file_hash as the field
                r.hset(job_id, file_hash, json_data)
            except TypeError as e:
                # Handle errors in data serialization
                print(f"Error serializing data for file_hash {file_hash}: {e}")
        return job_id
    except redis.RedisError as e:
        # Handle Redis-specific errors
        print(f"Redis error: {e}")
        return None
    except Exception as e:
        # Handle other generic errors
        print(f"An unexpected error occurred: {e}")
        return None
