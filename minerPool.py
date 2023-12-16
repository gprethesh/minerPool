import socket
import threading
import redis
import json
import config
import asyncio
import websockets


import os
import logging


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


async def periodic_process_transactions(api_url):
    print("Processing transactions")
    while True:
        process_transactions(api_url)
        await asyncio.sleep(20)


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

                        success, message = update_gradient(
                            job_name, just_name, "1", wallet_address, file_name
                        )
                        if success:
                            print("Inside of Success", success)
                            await websocket.send("FILE COMPLETE")

                        else:
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
    print("Requesting Job in MinerPool")
    start_server = websockets.serve(handle_client, config.IP, config.PORT)
    await start_server
    print(f"Server started on {config.IP}:{config.PORT}")

    # Start the periodic task
    periodic_task = asyncio.create_task(
        periodic_process_transactions("http://127.0.0.1:5500/transaction.json")
    )

    try:
        await asyncio.Future()  # Runs indefinitely
    finally:
        print("\n MinerPool shutdown process starting.")
        periodic_task.cancel()
        await periodic_task
        print("\n MinerPool shutdown process complete.")


# Start the main function using asyncio.run
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\n Shutting down MinerPool due to KeyboardInterrupt.")
