import pytest
import sys
import os
sys.path.append(os.path.abspath("../src"))
from change_stream_raw import format_json, send_to_insert_receiver
from change_stream_transformed import send_to_insert_receiver as send_to_insert_receiver_transformed
from mongomock import MongoClient
import asyncio
import logging

@pytest.fixture
def mongo_client():
    """Test Client for the mongodb test suite."""
    return MongoClient()

@pytest.fixture
def collection(mongo_client):
    """
    Test Collection for the mongodb test suite. Uses the mongo_client object to create a collection.
    """
    return mongo_client.test.RAW

def test_format_json():
    """
    Test the format_json function to ensure it formats the object correctly.
    This test runs the format_json function with a change object and verifies that:
    - The returned object contains the keys 'operationType', 'clusterTime', 'documentKey', and 'fullDocument'.
    - The returned object does not contain the keys '_id' or 'ns'.
    Logs the start and successful completion of the test.

    **Inputs:**
    - None

    **Outputs:**
    - None
    """
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
            "url": None,
            "text_length": 7,
            "text": "jwjvboa",
            "type": "txt"
        }
    }
    logging.info("test_format_json passed.")

def test_send_to_insert_receiver(collection):
    logging.info("Running test_send_to_insert_receiver...")
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
    asyncio.run(send_to_insert_receiver(change, 'insert'))
    logging.info("test_send_to_insert_receiver passed.")

def test_send_to_update_receiver(collection):
    logging.info("Running test_send_to_update_receiver...")
    change = {
        '_id': 
        {'_data': 'iur2o9uoubv3tbgsg65ps7'}, 
        'operationType': 'update', 
        'clusterTime': "Timestamp(1732081619, 1)", 'wallTime': "datetime.datetime(2024, 11, 20, 5, 46, 59, 470000)", 
        'fullDocument': {
            '_id': 'l8je93o2kzkrv3tbgsg65ps7', 
            'url': 'https://example.com/l8je93o2kzkrv3tbgsg65ps7', 
            'text_length': 7, 'text': 'jwjvboa', 'type': 'txt'}, 
            'ns': {'db': 'test', 'coll': 'RAW'}, 
            'documentKey': {'_id': 'l8je93o2kzkrv3tbgsg65ps7'}
    }
    asyncio.run(send_to_insert_receiver(change, 'update'))
    logging.info("test_send_to_update_receiver passed.")

def test_transformed_send_to_insert_reciever(collection):
    logging.info("Running test_transformed_send_to_update_receiver...")
    change = {
        '_id': 
        {'_data': 'iur2o9uoubv3tbgsg65ps7'}, 
        'operationType': 'update', 
        'clusterTime': "Timestamp(1732081619, 1)", 'wallTime': "datetime.datetime(2024, 11, 20, 5, 46, 59, 470000)", 
        'fullDocument': {
            '_id': 'l8je93o2kzkrv3tbgsg65ps7', 
            'url': 'https://example.com/l8je93o2kzkrv3tbgsg65ps7', 
            'text_length': 7, 'text': 'jwjvboa', 'type': 'txt'}, 
            'ns': {'db': 'test', 'coll': 'TRANSFORMED'}, 
            'documentKey': {'_id': 'l8je93o2kzkrv3tbgsg65ps7'}
    }
    asyncio.run(send_to_insert_receiver_transformed (change, 'insert'))
    
    logging.info("test_transformed_send_to_update_receiver passed.")
