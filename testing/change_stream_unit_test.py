import pytest
import sys
import os
sys.path.append(os.path.abspath("../src"))
from change_stream import format_json, send_to_insert_receiver
from mongomock import MongoClient
import asyncio
import logging

@pytest.fixture
def mongo_client():
    return MongoClient()

@pytest.fixture
def collection(mongo_client):
    return mongo_client.test.RAW

def test_format_json():
    logging.info("Running test_format_json...")
    change = {
        "operationType": "insert",
        "clusterTime": "2021-09-14T18:51:00.000Z",
        "documentKey": "1",
        "fullDocument": {
            "_id": "1",
            "url": "https://example.com/1",
            "text_length": 1000,
            "text": "text",
            "type": "html"
        }
    }
    json_object = asyncio.run(format_json(change))
    assert json_object == {
        "operationType": "insert",
        "clusterTime": "2021-09-14T18:51:00.000Z",
        "documentKey": "1",
        "fullDocument": {
            "_id": "1",
            "url": "https://example.com/1",
            "text_length": 1000,
            "text": "text",
            "type": "html"
        }
    }
    logging.info("test_format_json passed.")