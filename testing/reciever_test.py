import sys
import os
sys.path.append(os.path.abspath("../src"))
from reciever import raw__insert_receive, raw_update_receive, raw_delete_receive, app
import pytest
from flask import Flask
import logging


@pytest.fixture
def client():
    """Fixture for the Flask test client."""
    with app.test_client() as client:
        yield client

def test_raw__insert_receive(client):
    """Test the /RAW_insert_receiver route."""
    logging.info("Running test_raw__insert_receive...")
    response = client.post("/RAW_insert_receiver", json={"_id": "l8je93o2kzkrv3tbgsg65ps7", "url": "https://example.com/l8je93o2kzkrv3tbgsg65ps7", "text_length": 7, "text": "jwjvboa", "type": "txt"})
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Raw Data received successfully"}
    logging.info("test_raw__insert_receive passed.")

def test_raw_update_receive(client):
    """Test the /RAW_update_receiver route."""
    logging.info("Running test_raw_update_receive...")
    response = client.post("/RAW_update_receiver", json={"_id": "l8je93o2kzkrv3tbgsg65ps7", "url": "https://example.com/l8je93o2kzkrv3tbgsg65ps7", "text_length": 7, "text": "jwjvboa", "type": "txt"})
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Update Data received successfully"}
    logging.info("test_raw_update_receive passed.")

def test_raw_delete_receive(client):
    """Test the /RAW_delete_receiver route."""
    logging.info("Running test_raw_delete_receive...")
    response = client.post("/RAW_delete_receiver", json={"_id": "l8je93o2kzkrv3tbgsg65ps7", "url": "https://example.com/l8je93o2kzkrv3tbgsg65ps7", "text_length": 7, "text": "jwjvboa", "type": "txt"})
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Delete Data received successfully"}
    logging.info("test_raw_delete_receive passed.")
