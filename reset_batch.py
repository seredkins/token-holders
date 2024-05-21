from pymongo import MongoClient
from dotenv import dotenv_values
from datetime import datetime as dt
import logging

def reset_batch():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    config = dotenv_values(".env")
    client = MongoClient(config['MONGO_CONNECTION_STRING'])
    txs = client.get_database(config['MONGO_DB_NAME'])
    batch_collection = txs.get_collection(config['MONGO_BATCH_COLLECTION_NAME'])
    snapshots_collection = txs.get_collection(config['MONGO_SNAPSHOTS_COLLECTION_NAME'])
    txs_collection = txs.get_collection(config['MONGO_TXS_COLLECTION_NAME'])

    snapshot = {
        'datetime': dt.now(),
        'data': {}
    }
    if snapshots_collection.find_one() is not None:
        logger.info("Snapshots exist, fetching the latest one...")
        snapshot = snapshots_collection.find().sort("datetime", -1).limit(1)[0]

    for tx in batch_collection.find():
        token_address = tx['token_address']
        from_address = tx['from_address']
        to_address = tx['to_address']
        value = tx['value']
        
        if token_address not in snapshot['data'].keys():
            logger.info(f'New token address: {token_address}')
            snapshot['data'][token_address] = {
                from_address: 0,
                to_address: 0
            }
        else:
            if from_address not in snapshot['data'][token_address].keys():
                snapshot['data'][token_address][from_address] = 0
            if to_address not in snapshot['data'][token_address].keys():
                snapshot['data'][token_address][to_address] = 0

        try:
            a, b = snapshot['data'][token_address][from_address], snapshot['data'][token_address][to_address]
            a = int(a) - int(value)
            b = int(b) + int(value)
            snapshot['data'][token_address][from_address] = str(a)
            snapshot['data'][token_address][to_address] = str(b)
        except Exception as e:
            print(e, a, b, value)
    
    snapshots_collection.insert_one(snapshot)
    txs_collection.insert_many(batch_collection.find())
    batch_collection.delete_many({})

if __name__ == "__main__":
    reset_batch()

        