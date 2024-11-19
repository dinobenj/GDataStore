from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
import json
import asyncio
import requests
import random
import string

async def generate_json_objects(num_objects):
    json_objects = []
    for _ in range(num_objects):
        
        object_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))
        url = f"https://example.com/{object_id}"
        text = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1000, 10000)))
        t_type = random.choice(["type1", "type2", "type3"])
        json_obj = {
            "_id": str(object_id),
            "url": url,
            "text_length": len(text) if text else 0,
            "text": text,
            "type": t_type
        }
        json_objects.append(json_obj)
    return json_objects


async def add_one(json_obj: dict, collection):
    try:
        result = await collection.insert_one(json_obj)
        logging.info(f"Added document with id: {result.inserted_id}")
    except PyMongoError as e:
        logging.error(f"Error adding document: {e}")

async def add_many(json_objects, collection):
    try:
        result = collection.insert_many(json_objects)
        logging.info(f"Added {len(result.inserted_ids)} documents.")
        print(f"Added {len(result.inserted_ids)} documents.")
    except PyMongoError as e:
        logging.error(f"Error adding documents: {e}")


async def addData(page_data:string, page_id:string, collection) -> None:
    try:
        result = await collection.insert_one({"page_id": page_id, "page_data": page_data})
        logging.info(f"Added document with id: {result.inserted_id}")
    except PyMongoError as e:
        logging.error(f"Error adding document: {e}")

async def notifyQuery(new_document_ids: list) -> None:
    try:
        logging.info(f"Added documents with id(s): {new_document_ids}")
    except PyMongoError as e:
        logging.error(f"Error adding document: {e}")

async def removeData(url: string, collection) -> None:
    try:
        result = await collection.delete_one({"url": url})
        logging.info(f"Removed document with id: {result.deleted_id}")
    except PyMongoError as e:
        logging.error(f"Error removing document: {e}")
async def updateRawData(url: string, new_data: string, collection) -> None:
    try:
        result = await collection.update_one({"url": url}, {"$set": {"text": new_data}})
        logging.info(f"Updated document with id: {result.upserted_id}")
    except PyMongoError as e:
        logging.error(f"Error updating document: {e}")




if __name__ == '__main__':
    client = MongoClient("mongodb://localhost:27017")
    print("Listening...")
    db = client.test
    collection = db.RAW
    delete = collection.delete_many({})

    json_objects = asyncio.run(generate_json_objects(1000))
    asyncio.run(add_many(json_objects, collection))