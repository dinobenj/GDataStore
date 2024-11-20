import pytest
import sys
import os
sys.path.append(os.path.abspath("../src"))
from util import generate_json_objects, add_many, notifyQuery, removeData
from mongomock import MongoClient
import asyncio
import logging

@pytest.fixture
def mongo_client():
    return MongoClient()

@pytest.fixture
def collection(mongo_client):
    return mongo_client.test.RAW

def test_generate_json_objects():
    logging.info("Running test_generate_json_objects...")
    json_objects = asyncio.run(generate_json_objects(1))
    assert len(json_objects) == 1
    assert 'text' in json_objects[0]
    assert 'url' in json_objects[0]
    assert 'type' in json_objects[0]
    assert 'text_length' in json_objects[0]
    logging.info("test_generate_json_objects passed.")

def test_add_many(collection):
    logging.info("Running test_add_many...")
    json_objects = asyncio.run(generate_json_objects(1))
    asyncio.run(add_many(json_objects, collection))
    assert collection.count_documents({}) == 1
    logging.info("test_add_many passed.")

def test_notifyQuery(collection):
    logging.info("Running test_notifyQuery...")
    asyncio.run(add_many(asyncio.run(generate_json_objects(1)), collection))
    asyncio.run(notifyQuery([1]))
    assert collection.count_documents({}) == 1
    logging.info("test_notifyQuery passed.")

def test_removeData(collection):
    logging.info("Running test_removeData...")
    asyncio.run(add_many(asyncio.run(generate_json_objects(1)), collection))
    url = collection.find_one({})["url"]
    asyncio.run(removeData(url, collection))
    assert collection.count_documents({}) == 0
    logging.info("test_removeData passed.")

