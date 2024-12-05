import pymongo
from pymongo.errors import PyMongoError
from dotenv import load_dotenv
import os
import json
import requests 
import logging

def send_to_evaluator(json_object):
    try:
        print("Sending object to evaluator...")
        response = requests.post(os.getenv("EVALUATION_ENDPOINT"), json=json_object)
        print("Response from evaluator:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error sending data to evaluator: {e}")

def main():
    load_dotenv()
    try:
        client = pymongo.MongoClient(os.getenv("MONGO_URI"))
        print(client.list_database_names())  # List databases to verify connection
    except Exception as e:
        print(f"Error: {e}")

    client = pymongo.MongoClient(os.getenv("MONGO_URI"))
    print("Starting cron job...")
    db = client.test
    raw_collection = db.test.RAW
    transformed_collection = db.TRANSFORMED
    raw_collection_count = raw_collection.count_documents({})
    print(f"Number of documents in RAW collection: {raw_collection_count}")
    transformed_collection_count = transformed_collection.count_documents({})
    print(f"Number of documents in TRANSFORMED collection: {transformed_collection_count}")
    send_to_evaluator({"raw_collection_count": raw_collection_count, "transformed_collection_count": transformed_collection_count})

if __name__ == '__main__':
    main()

