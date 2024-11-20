import pytest
import sys
import os
sys.path.append(os.path.abspath("../src"))
from util import generate_json_objects, add_many, notifyQuery, removeData
from mongomock import MongoClient
import asyncio
import logging


"""

"""
@pytest.fixture
def mongo_client():
    return MongoClient()

@pytest.fixture
def collection(mongo_client):
    return mongo_client.test.RAW

def test_generate_json_objects():
    """
    **Unit Test for generate_json_objects**

    Test the generate_json_objects function to ensure it generates the correct JSON objects.
    This test runs the generate_json_objects function with an input of 1 and verifies that:
    - The length of the returned list is 1.
    - Each JSON object in the list contains the keys 'text', 'url', 'type', and 'text_length'.
    Logs the start and successful completion of the test.

    **Inputs:**
    - None
    **Outputs:**
    - Nonne

    All Logging is sent to stderr as opposed to stdout.
    """
    logging.info("Running test_generate_json_objects...")
    json_objects = asyncio.run(generate_json_objects(1))
    assert len(json_objects) == 1
    assert 'text' in json_objects[0]
    assert 'url' in json_objects[0]
    assert 'type' in json_objects[0]
    assert 'text_length' in json_objects[0]
    logging.info("test_generate_json_objects passed.")

def test_add_many(collection):
    """
    **Unit Test for add_many**
    Test that the add_many function adds the correct number of JSON objects to the collection.
    This test runs the add_many function with an input of 1 JSON object and verifies that:
    - The collection contains 1 document.
    Logs the start and successful completion of the test.
    **Inputs:**
    - collection: A collection object from the mongomock library used to mimick mongo functionality.

    **Outputs:**
    - None

    All Logging is sent to stderr as opposed to stdout.
    """
    logging.info("Running test_add_many...")
    json_objects = asyncio.run(generate_json_objects(1))
    asyncio.run(add_many(json_objects, collection))
    assert collection.count_documents({}) == 1
    logging.info("test_add_many passed.")

def test_notifyQuery(collection):
    """
    **Unit Test for notifyQuery**
    Test that the notifyQuery function adds the correct number of JSON objects to the collection.
    This test runs the notifyQuery function with an input of [1] and verifies that:
    - The collection contains 1 document.
    Logs the start and successful completion of the test.
    **Inputs:**
    - collection: A collection object from the mongomock library used to mimick mongo functionality.

    **Outputs:**
    - None

    All Logging is sent to stderr as opposed to stdout.
    """
    logging.info("Running test_notifyQuery...")
    asyncio.run(add_many(asyncio.run(generate_json_objects(1)), collection))
    asyncio.run(notifyQuery([1]))
    assert collection.count_documents({}) == 1
    logging.info("test_notifyQuery passed.")

def test_removeData(collection):
    """

    **Unit Test for removeData**
    Test that the removeData function removes the correct number of JSON objects from the collection.
    This test runs the removeData function with an input of a URL and verifies that:
    - The collection contains 0 documents.
    Logs the start and successful completion of the test.
    **Inputs:**
    - collection: A collection object from the mongomock library used to mimick mongo functionality.

    **Outputs:**
    - None

    All Logging is sent to stderr as opposed to stdout.
    """
    logging.info("Running test_removeData...")
    asyncio.run(add_many(asyncio.run(generate_json_objects(1)), collection))
    url = collection.find_one({})["url"]
    asyncio.run(removeData(url, collection))
    assert collection.count_documents({}) == 0
    logging.info("test_removeData passed.")

