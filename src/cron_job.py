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
    print(os.getenv("MONGO_URI"))
    MongoClient = pymongo.MongoClient(os.getenv("MONGO_URI"))

    db = MongoClient.test
    raw_collection = db.RAW
    transformed_collection = db.TRANSFORMED
    print("Starting cron job...")
    raw_documents = raw_collection.count_documents({})
    transformed_documents = transformed_collection.count_documents({})
    print(f"RAW collection has {raw_documents} documents")
    print(f"TRANSFORMED collection has {transformed_documents} documents")
    send_to_evaluator({"raw_documents": raw_documents, "transformed_documents": transformed_documents})

if __name__ == '__main__':
    main()

