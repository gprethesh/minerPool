import redis
import json
import datetime
import os

from database import r
from updateMiner import update_miner
from model import load_model_from_pth


def delete_file_on_error(jobname, file_name):
    job_folder_path = os.path.join("Job", jobname)
    file_path = os.path.join(job_folder_path, file_name)

    if not os.path.exists(file_path):
        return False, f"File {file_name} not found in {jobname}."

    try:
        os.remove(file_path)
        # Removing the job folder if it's empty, using os.rmdir instead of shutil.rmtree
        if not os.listdir(job_folder_path):
            os.rmdir(job_folder_path)
        return True, f"File {file_name} successfully deleted from {jobname}."
    except Exception as e:
        return False, f"Error deleting file: {e}"


# def update_gradient(jobname, hash_value, new_gradient, wallet_address, file_name):
#     try:
#         if not r.exists(jobname):
#             return False, f"Job {jobname} not found in the database."

#         job_data = r.hget(jobname, hash_value)
#         if not job_data:
#             return False, f"Hash {hash_value} not found in job {jobname}."

#         try:
#             data = json.loads(job_data)
#         except json.JSONDecodeError:
#             return (
#                 False,
#                 f"Error decoding JSON data for hash {hash_value} in job {jobname}.",
#             )

#         if data.get("gradient", 0) != 0:
#             return (
#                 False,
#                 f"Gradient already exists for hash {hash_value} in job {jobname}.",
#             )

#         if data.get("downloaded", 0) == 0:
#             return False, f"This job {hash_value} wasn't downloaded."

#         data["gradient"] = new_gradient
#         updated_data = json.dumps(data)
#         r.hset(jobname, hash_value, updated_data)

#         print("Came till here")
#         try:
#             current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             success, message = update_miner(wallet_address, "1", current_time)
#             if not success:
#                 return False, message
#         except Exception as e:
#             return False, f"Error updating miner: {e}"

#         return True, "Gradient updated successfully."

#     except redis.RedisError as e:
#         return False, f"Redis error: {e}"
#     except Exception as e:
#         return False, f"An error occurred: {e}"


def update_gradient(jobname, hash_value, new_gradient, wallet_address, file_name):
    try:
        if not r.exists(jobname):
            return False, f"Job {jobname} not found in the database."

        job_data = r.hget(jobname, hash_value)
        if not job_data:
            return False, f"Hash {hash_value} not found in job {jobname}."

        try:
            data = json.loads(job_data)
            print("json success")
        except json.JSONDecodeError:
            delete_file_on_error(jobname, file_name)  # Delete file on JSON decode error
            return (
                False,
                f"Error decoding JSON data for hash {hash_value} in job {jobname}.",
            )

        if data.get("gradient", 0) != 0:
            print("gradient erorr so file got deleted")
            delete_file_on_error(
                jobname, file_name
            )  # Delete file if gradient already exists
            return (
                False,
                f"Gradient already exists for hash {hash_value} in job {jobname}.",
            )

        if data.get("downloaded", 0) == 0:
            print("file wasn't downloaed so it got deleted")
            delete_file_on_error(
                jobname, file_name
            )  # Delete file if job was not downloaded
            return False, f"This job {hash_value} wasn't downloaded."

        try:
            model_path = f"Job/{jobname}/{file_name}"
            models = []
            model = load_model_from_pth(model_path)
            models.append(model)
            print("Model loaded successfully", models)
        except FileNotFoundError:
            print(f"File not found: {model_path}")
            # Additional file not found handling logic here, if needed
        except IOError:
            print(f"IO error occurred while loading the model from {model_path}")
            # Additional IOError handling logic here, if needed
        except Exception as e:
            print(f"Error loading model from {model_path}: {e}")
            # Additional general exception handling logic here, if needed
            delete_file_on_error(jobname, file_name)
            return False, f"This job {hash_value} was corrupted."

        # Proceed with updating gradient
        data["gradient"] = new_gradient
        updated_data = json.dumps(data)
        r.hset(jobname, hash_value, updated_data)

        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        success, message = update_miner(wallet_address, "1", current_time)
        if not success:
            delete_file_on_error(
                jobname, file_name
            )  # Delete file if updating miner fails
            return False, message

        file_hashes = r.hgetall(jobname)
        if not file_hashes:
            print(f"Error: No job found with ID {jobname}.")
            return None, None

        # Initialize a flag to track if any gradient is 0
        gradient_missing = False

        for key, value in file_hashes.items():
            # Check if value needs decoding
            if isinstance(value, bytes):
                value = value.decode("utf-8")

            try:
                data = json.loads(value)  # Load JSON data
            except json.JSONDecodeError:
                print("Error decoding")
                continue  # Skip to the next item

            # Check if 'gradient' is missing or 0
            if "gradient" not in data or data["gradient"] == 0:
                gradient_missing = True
                break  # Exit loop if any gradient is 0 or missing

        # Use the gradient_missing flag as needed
        if gradient_missing:
            print("Gradient missing")
        else:
            print("No gradient missing")

        return True, "Gradient updated successfully."

    except redis.RedisError as e:
        delete_file_on_error(jobname, file_name)  # Delete file on Redis error
        return False, f"Redis error: {e}"
    except Exception as e:
        delete_file_on_error(jobname, file_name)  # Delete file on any other error
        return False, f"An error occurred: {e}"
