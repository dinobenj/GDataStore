from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
import json
import asyncio
import requests
import os
from dotenv import load_dotenv

async def format_json(change):
    print(change)
    change_type = change['operationType']
    cluster_time = change['clusterTime']
    object_id = change['documentKey']['_id']
    print(change)
    url = change['fullDocument']['url'] if 'fullDocument' in change else None
    text = change['fullDocument']['text'] if 'fullDocument' in change else None
    t_type = change['fullDocument']['type'] if 'fullDocument' in change else None
    json_object = {
        "operationType": change_type,
        "clusterTime": cluster_time,
        "documentKey": object_id,
        "fullDocument": {
            "_id": str(object_id),
            "url": url,
            "text_length": len(text) if text else 0,
            "text": text,
            "type": t_type
        }
    }
    return json_object

async def send_to_insert_receiver(change, change_type):
    print(f"TRANSFORMED collection change detected of type: {change_type.upper()}")
    if change_type == 'insert':
        print("Insert detected")
        try:
            json_object = await format_json(change)
            internal_id = json_object["fullDocument"]["_id"]
            print(f"Sending object with internal '_id' {internal_id} to receiver...")
            json_object = json.dumps(json_object, indent=2, default=str)

            response = requests.post("http://localhost:5000/TRANSFORMED_insert_receiver", json=json_object)
            print("Response from receiver:", response.json())
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending data to receiver: {e}")
    if change_type == 'update':
        try:
            print("Update detected")
            json_object = await format_json(change)
            internal_id = json_object["fullDocument"]["_id"]
            print(f"Sending object with internal '_id' {internal_id} to receiver...")
            json_object = json.dumps(json_object, indent=2, default=str)
            response = requests.post("http://localhost:5000/TRANSFORMED_update_receiver", json=json_object)
            print("Response from receiver:", response.json())
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending data to receiver: {e}")

    if change_type == 'delete':
        try:
            print("Delete detected")
            json_object = await format_json(change)
            internal_id = json_object["documentKey"]
            print(f"Sending object with internal '_id' {internal_id} to receiver...")
            json_object = json.dumps(json_object, indent=2, default=str)
            response = requests.post("http://localhost:5000/TRANSFORMED_delete_receiver", json=json_object)
            print("Response from receiver:", response.json())
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending data to receiver: {e}")

def main():
    load_dotenv()
    client = MongoClient(os.getenv("MONGO_URI"))
    print("Listening...")
    db = client.test
    raw_collection = db.TRANSFORMED

    try:
        with raw_collection.watch([{"$match": {"operationType": {"$in": ["insert", "update", "delete"]}}}]) as raw_change_stream:
            for change in raw_change_stream:
                change_type = change['operationType']
                asyncio.run(send_to_insert_receiver(change, change_type))
    except PyMongoError as e:
        logging.error(f"Error watching change stream: {e}")


if __name__ == '__main__':
    main()  