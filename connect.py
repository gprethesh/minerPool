import asyncio
import websockets
import threading
import json
import time
from database import r
import logging
import config
import requests
from datetime import datetime, timedelta
import signal
import sys
from model import update_model_record, check_model_record


class MessageType:
    VALIDATEMODEL = "validateModel"
    UPDATEMODEL = "updateModel"


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.getLogger("websockets").setLevel(logging.INFO)


from pick import pick_model_for_processing

websockets_dict = {}


def read_peers(file_path):
    valid_peers = []
    try:
        with open(file_path, "r") as file:
            data = json.load(file)

            for wallet_address, details in data.items():
                ip = details.get("IP")
                port = details.get("Port")

                if ip and port:
                    uri = f"ws://{ip}:{port}"
                    print(f"Read URI: '{uri}'")  # Print the URI for debugging
                    valid_peers.append(uri)
                else:
                    print(f"Missing IP or Port for wallet {wallet_address}")

    except json.JSONDecodeError:
        print("Error decoding JSON from the file")
    except FileNotFoundError:
        print(f"File not found: {file_path}")

    return valid_peers


def read_wallet(wallet_address):
    try:
        with open("peers.json", "r") as file:
            data = json.load(file)

            for address, details in data.items():
                if address == wallet_address:
                    percentage = details.get("Percentage")
                    if percentage is not None:
                        print(f"Wallet: {wallet_address}, Percentage: {percentage}%")
                        return percentage
                    else:
                        print(f"Percentage missing for wallet {wallet_address}")
                        return None

        print(f"Wallet address {wallet_address} not found.")
        return None

    except json.JSONDecodeError:
        print("Error decoding JSON from the file")
        return None
    except FileNotFoundError:
        print("File not found: peer.json")
        return None


def save_valid_peers_to_json(vals):
    current_time = datetime.now()
    four_hours_ago = current_time - timedelta(hours=4)
    valid_peers = {}

    for wallet_address, details in vals.items():
        details_dict = json.loads(details)

        # Check if percentage is 1 or more
        if details_dict.get("percentage", 0) >= 1:
            # Parse the ping time
            ping_time = details_dict.get("ping")
            if ping_time:
                try:
                    ping_datetime = datetime.fromisoformat(ping_time)

                    # Check if ping is within the last 4 hours
                    if ping_datetime >= four_hours_ago:
                        # Add the wallet address and its details to the JSON object
                        valid_peers[wallet_address] = {
                            "Percentage": details_dict["percentage"],
                            "IP": details_dict["ip"],
                            "Port": details_dict["port"],
                        }
                except ValueError:
                    # Handle invalid date format
                    print(f"Invalid date format for wallet {wallet_address}")

    # Write the JSON object to a file
    with open("peers.json", "w") as file:
        json.dump(valid_peers, file, indent=4)


def fetch_validators(validators):
    try:
        response = requests.get(validators)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        # print("Validators Found", response.json())
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    return []


def model_processing_thread(queue, interval=60):
    logging.info("Model processing thread started")
    while True:
        model_data = pick_model_for_processing()
        if model_data:
            logging.debug(f"Model data received: {model_data}")
            queue.put_nowait(model_data)
            logging.debug(f"Model data queued: {model_data}")
        time.sleep(interval)


async def connect_to_server(uri, queue):
    while True:  # Outer loop for reconnection
        try:
            async with websockets.connect(uri) as websocket:
                websockets_dict[uri] = websocket
                logging.info(f"Connected to {uri}")

                async def send_messages():
                    while True:
                        try:
                            job_id = await asyncio.wait_for(queue.get(), timeout=30)
                        except asyncio.TimeoutError:
                            logging.warning(f"No job {job_id} was received for {uri}")
                            continue

                        if job_id is None:
                            break

                        message = json.dumps(
                            {
                                "job_id": job_id,
                                "miner_pool_wallet": config.WALLET_ADDRESS,
                                "validator_wallet": config.VALIDATOR_WALLET_ADDRESS,
                                "job_details": "Job completed",
                                "type": MessageType.VALIDATEMODEL,
                            }
                        )

                        ws = websockets_dict.get(uri)
                        # print("insdide of ws", ws)
                        # print("ws.open", ws.open)
                        if ws and ws.open:
                            # print("successfully inside ws.open", ws.open)
                            try:
                                if not check_model_record(
                                    job_id, config.VALIDATOR_WALLET_ADDRESS
                                ):
                                    await ws.send(message)
                                    logging.debug(f"Sent JSON message to {uri}")
                            except Exception as e:
                                logging.error(f"Error in sending to {uri}: {e}")
                                # Remove the invalid WebSocket and trigger reconnection
                                websockets_dict.pop(uri, None)
                                break  # Exit send_messages to trigger reconnection
                        else:
                            logging.error("WebSocket not found in websockets_dict")
                            websockets_dict.pop(uri, None)
                            break

                async def receive_messages():
                    while True:
                        try:
                            if websocket.open:
                                incoming_message = await websocket.recv()
                                logging.info(
                                    f"Received message from server: {incoming_message}"
                                )
                                parsed_message = json.loads(incoming_message)
                                message_type = parsed_message.get("type")

                                if message_type == MessageType.UPDATEMODEL:
                                    job_id = parsed_message.get("job_id")
                                    validator_wallet = parsed_message.get(
                                        "validator_wallet"
                                    )
                                    percentage = read_wallet(validator_wallet)

                                    update = update_model_record(
                                        job_id, percentage, validator_wallet
                                    )
                                    print(
                                        "percentage",
                                        percentage,
                                        job_id,
                                        validator_wallet,
                                    )
                                    print("update", update)

                        except websockets.ConnectionClosed:
                            break

                send_task = asyncio.create_task(send_messages())
                receive_task = asyncio.create_task(receive_messages())
                await asyncio.gather(send_task, receive_task)

        except Exception as e:
            logging.error(f"Error with WebSocket connection to {uri}: {e}")
            websockets_dict.pop(uri, None)

        await asyncio.sleep(10)  # Wait before trying to reconnect


def start_connection(uri, queue):
    asyncio.new_event_loop().run_until_complete(connect_to_server(uri, queue))


def main():
    try:
        vals = fetch_validators(config.INODE_VALIDATOR_LIST)
        print("vals", vals)
        save_valid_peers_to_json(vals)
        peers = read_peers("peers.json")
        message_queue = asyncio.Queue()

        model_thread = threading.Thread(
            target=model_processing_thread, daemon=True, args=(message_queue,)
        )
        model_thread.start()

        connection_threads = []

        for uri in peers:
            thread = threading.Thread(
                target=start_connection, daemon=True, args=(uri, message_queue)
            )
            thread.start()
            connection_threads.append(thread)

        # Wait for model thread to complete
        model_thread.join()

        # Wait for all connection threads to complete
        for thread in connection_threads:
            thread.join()

    except KeyboardInterrupt:
        print("\n Shutting down...")
        # Perform any necessary cleanup here
        sys.exit(0)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        # Handle other exceptions as needed
        sys.exit(1)


if __name__ == "__main__":
    main()
