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
        '_id': 
        {'_data': 'iur2o9uoubv3tbgsg65ps7'}, 
        'operationType': 'insert', 
        'clusterTime': "Timestamp(1732081619, 1)", 'wallTime': "datetime.datetime(2024, 11, 20, 5, 46, 59, 470000)", 
        'fullDocument': {
            '_id': 'l8je93o2kzkrv3tbgsg65ps7', 
            'url': 'https://example.com/l8je93o2kzkrv3tbgsg65ps7', 
            'text_length': 7, 'text': 'jwjvboa', 'type': 'txt'}, 
            'ns': {'db': 'test', 'coll': 'RAW'}, 
            'documentKey': {'_id': 'l8je93o2kzkrv3tbgsg65ps7'}
    }
        
    json_object = asyncio.run(format_json(change))
    assert json_object == {
        "operationType": "insert",
        "clusterTime": "Timestamp(1732081619, 1)",
        "documentKey": 'l8je93o2kzkrv3tbgsg65ps7',
        "fullDocument": {
            "_id": "l8je93o2kzkrv3tbgsg65ps7",
            "url": "https://example.com/l8je93o2kzkrv3tbgsg65ps7",
            "text_length": 7,
            "text": "jwjvboa",
            "type": "txt"
        }
    }
    logging.info("test_format_json passed.")