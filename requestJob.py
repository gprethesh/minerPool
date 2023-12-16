import socket
import config
import requests
import io
import csv
import json
import logging

from createJob import create_job
from activeMinig import active_mining, mining_status


class MessageType:
    GRADIENT = "gradient"
    REQUESTFILE = "requestFile"
    REQUESTJOB = "requestJob"


def read_csv_and_parse(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        file_like_object = io.StringIO(response.text)
        csv_reader = csv.reader(file_like_object)

        # Skip the header row (first row)
        next(csv_reader, None)  # This skips the first row

        file_hashes = {}
        for row in csv_reader:
            # Check if the row has exactly 2 elements
            if len(row) == 2:
                file_url, sha256_hash = row
                file_hashes[sha256_hash] = {
                    "wallet": 0,
                    "downloaded": 0,
                    "last_active": 0,
                    "gradient": 0,
                    "location": 0,
                    "url": file_url,
                }
            else:
                print(f"Skipping invalid row: {row}")

        return file_hashes

    except requests.HTTPError as e:
        print(f"HTTP request error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def request_job(wallet_id, message_type):
    try:
        print("requesting job")

        # Validate message type
        if message_type != MessageType.REQUESTJOB:
            raise ValueError("Invalid message type")

        # Structure data based on message type
        data = {
            "type": message_type,
            "content": {
                "wallet_id": wallet_id,
            },
        }

        serialized_data = json.dumps(data)

        # Create a socket object
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect to the server with a timeout
        client.settimeout(10)  # Set timeout in seconds
        client.connect((config.INODE_IP, config.INODE_PORT))

        # Send the serialized data
        client.sendall(serialized_data.encode("utf-8"))

        # Wait for the server's response
        response = client.recv(config.BUFFER_SIZE)

        # Check if response is empty
        if not response:
            print("Received an empty response from the server")
            return None

        # Try to decode the response
        decoded_response = response.decode("utf-8")
        print("received message", decoded_response)
        response_data = json.loads(decoded_response)

        # Process the response
        hash_url = response_data.get("hash")

        # Check for valid hash_url
        if not hash_url:
            raise ValueError("Received invalid hash URL")

        file_hashes = read_csv_and_parse(hash_url)

        if file_hashes:
            job_id = response_data.get("jobname")
            value = create_job(job_id, file_hashes)
            if value:
                result = active_mining(value)
                status = mining_status(True)
                print("result", result, status)
            else:
                raise RuntimeError("Failed to create job")
            return response_data
        else:
            raise RuntimeError("Failed to create job due to empty file hashes")

    except socket.timeout:
        print("Connection timed out")
    except socket.error as e:
        print("Socket error:", e)
    except json.JSONDecodeError:
        print("Error decoding JSON")
    except ValueError as ve:
        print(ve)
    except Exception as e:
        print("An unexpected error occurred:", e)
    finally:
        if "client" in locals():
            client.close()

    return None
