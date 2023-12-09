import leveldb
import json


def store_in_leveldb(block_height, miner_updates):
    try:
        db = leveldb.LevelDB("./miner_balance_updates")
        try:
            db.Put(str(block_height).encode(), json.dumps(miner_updates).encode())
            print(f"Successfully stored updates for block height {block_height}.")
        except Exception as e:
            print(f"Error storing data in LevelDB for block height {block_height}: {e}")
    except Exception as e:
        print(f"Error initializing LevelDB database: {e}")


def retrieve_from_leveldb(block_height):
    try:
        db = leveldb.LevelDB("./miner_balance_updates")
        try:
            print("hello")
            data = db.Get(str(block_height).encode())
            miner_updates = json.loads(data.decode())
            print("Successfully retrieved", miner_updates)
            return miner_updates
        except KeyError:
            print(f"No data found for block height {block_height}.")
        except Exception as e:
            print(
                f"Error retrieving data from LevelDB for block height {block_height}: {e}"
            )
    except Exception as e:
        print(f"Error initializing LevelDB database: {e}")

    return None
