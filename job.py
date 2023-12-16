import redis
import json
import datetime

from database import r


def create_job(job_id, file_hashes):
    try:
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


def get_job_data(job_id, file_hashes):
    retrieved_data = {}
    for file_hash in file_hashes:
        # Get the JSON string stored in Redis
        json_data = r.hget(job_id, file_hash)
        # Deserialize the JSON string back to a Python dictionary
        if json_data:
            data = json.loads(json_data)
            retrieved_data[file_hash] = data
    return retrieved_data
