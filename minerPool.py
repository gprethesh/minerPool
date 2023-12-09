import socket
import threading
import redis
import json
import config
import asyncio
import websockets
import requests
import csv
import io
import os
import logging

from createJob import create_job
from activeMinig import active_mining, mining_status
from updateJob import update_jobs
from updateGradient import update_gradient
from fetchBlock import process_transactions


class MessageType:
    GRADIENT = "gradient"
    REQUESTFILE = "requestFile"
    REQUESTJOB = "requestJob"


# Constants
IP = socket.gethostbyname(socket.gethostname())
PORT = 65431
BUFFER_SIZE = 1024


def request_job(wallet_id, message_type):
    print("Request job called")
    # Structure data based on message type
    if message_type == MessageType.REQUESTJOB:
        data = {
            "type": MessageType.REQUESTJOB,
            "content": {
                "wallet_id": wallet_id,
            },
        }
    else:
        raise ValueError("Invalid message type")

    serialized_data = json.dumps(data)

    # Create a socket object
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to the server
    client.connect((config.INODE_IP, config.INODE_PORT))

    # Send the serialized data
    client.send(serialized_data.encode("utf-8"))

    print("sent message")

    # Wait for the server's response
    response = client.recv(config.BUFFER_SIZE).decode("utf-8")

    print("received message", response)

    response_data = json.loads(response)
    hash_url = response_data.get("hash")
    file_hashes = read_csv_and_parse(hash_url)

    if file_hashes:
        job_id = response_data.get("jobname")
        value = create_job(job_id, file_hashes)
        if value:
            result = active_mining(value)
            status = mining_status(True)
            print("result", result, status)
        else:
            print("failed to create job")
        return response
    else:
        print("failed to create job")


def read_csv_and_parse(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        file_like_object = io.StringIO(response.text)
        csv_reader = csv.reader(file_like_object)

        file_hashes = {}
        for row in csv_reader:
            # Assuming the first element is the URL and the second is the SHA256 hash
            file_url, sha256_hash = row
            file_hashes[sha256_hash] = {
                "wallet": 0,
                "downloaded": 0,
                "last_active": 0,
                "gradient": 0,
                "location": 0,
                "url": file_url,
            }

        return file_hashes

    except requests.HTTPError as e:
        print(f"HTTP request error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


async def periodic_process_transactions(api_url):
    print("Processing transactions")
    while True:
        process_transactions(api_url)
        await asyncio.sleep(20)


# def handle_clientX(client_socket, client_address):
#     try:
#         # Receive data from client
#         data = client_socket.recv(BUFFER_SIZE).decode("utf-8")
#         data = json.loads(data)

#         print("DATA FROM client", data)
#         response_message = "Data processed successfully"
#         client_socket.send(response_message.encode("utf-8"))
#     except Exception as e:
#         print(f"Error handling client: {e}")
#         client_socket.send(f"Error: {str(e)}".encode("utf-8"))
#     finally:
#         client_socket.close()


# def start_server():
#     server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server.bind((IP, PORT))
#     server.listen(5)
#     print(f"Server started on {IP}:{PORT}")
#     request_job(config.WALLET_ADDRESS, MessageType.REQUESTJOB)

#     while True:
#         client_socket, client_address = server.accept()
#         print(f"Accepted connection from {client_address}")
#         client_handler = threading.Thread(
#             target=handle_clientX, args=(client_socket, client_address)
#         )
#         client_handler.start()


# Start the server
# start_server()

# request_job(config.WALLET_ADDRESS, MessageType.REQUESTJOB)


def save_file_chunk_in_job_folder(chunk, folder_name, file_name, is_first_chunk):
    # Extract the last part of the folder_name
    folder_name = os.path.basename(folder_name)

    # Create the full path dynamically
    folder_path = os.path.join("Job", folder_name)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, file_name)

    # Save the file chunk
    with open(file_path, "ab" if not is_first_chunk else "wb") as file:
        file.write(chunk)


async def handle_client(websocket, path):
    try:
        async for message in websocket:
            try:
                parsed_message = json.loads(message)
                message_type = parsed_message.get("type")

                if message_type == "gradient":
                    folder_name = parsed_message.get("folder_name")
                    job_name = parsed_message.get("job_name")
                    wallet_address = parsed_message.get("wallet_address")
                    file_name = parsed_message.get("file_name")
                    just_name = parsed_message.get("just_name")
                    file_chunk = parsed_message.get("file_data").encode("latin1")

                    if file_chunk == b"EOF":
                        print(f"Completed receiving {file_name}")
                        print("just_name", just_name)
                        success, message = update_gradient(
                            job_name, just_name, "1", wallet_address, file_name
                        )
                        if success:
                            print("Inside of Success", success)
                            await websocket.send("FILE COMPLETE")

                        else:
                            print("Inside of Failure")
                            await websocket.send(f"ERROR: {message}")
                    else:
                        is_first_chunk = parsed_message.get("is_first_chunk", False)
                        save_file_chunk_in_job_folder(
                            file_chunk, folder_name, file_name, is_first_chunk
                        )

                elif message_type == "requestFile":
                    wallet_address = parsed_message.get("wallet_address")
                    file = update_jobs(wallet_address)
                    print("file", file)
                    if file is None:
                        await websocket.send("NO JOB FOUND!")
                    else:
                        await websocket.send(json.dumps(file))
                else:
                    await websocket.send("Unknown message type")
            except json.JSONDecodeError:
                await websocket.send("Invalid message format")

    except websockets.ConnectionClosed:
        print("Client disconnected")
        # Handle disconnection


async def main():
    # Start the WebSocket server
    start_server = websockets.serve(handle_client, config.IP, config.PORT)
    await start_server
    print(f"Server started on {config.IP}:{config.PORT}")

    # Start the periodic task
    periodic_task = asyncio.create_task(
        periodic_process_transactions("http://127.0.0.1:5500/transaction.json")
    )

    print("Starting periodic", periodic_task)

    try:
        await asyncio.Future()  # Runs indefinitely until an exception occurs
    except KeyboardInterrupt:
        print("MinerPool shutdown process starting.")
        periodic_task.cancel()
        await periodic_task
        print("MinerPool shutdown process complete.")


# Start the main function using asyncio.run
asyncio.run(main())
