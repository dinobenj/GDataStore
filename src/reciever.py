from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import requests
from dotenv import load_dotenv
import os
from json import loads
app = Flask(__name__)

@app.route('/RAW_insert_receiver', methods=['POST'])
def raw__insert_receive():
    data = request.json  
    print("Received JSON data:", data)
    print("Sending to Transformation...")
    response = {"status": "success", "message": "Raw Data received successfully"}
    print("gonna send this info to transformation endpoint...")
    endpoint = os.getenv("TEXT_TRANSFORMATION_ENDPOINT") + "/newDocument"
    data_dict = loads(data)
    doc_id = data_dict["fullDocument"]["_id"]
    d = {"document_id": doc_id}
    response = requests.post(endpoint, json=d)
    return response.text, 200

@app.route('/RAW_update_receiver', methods=['POST'])
def raw_update_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Update Data received successfully"}
    return jsonify(response), 200

@app.route('/RAW_delete_receiver', methods=['POST'])
def raw_delete_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Delete Data received successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

@app.route('/TRANSFORMED_insert_receiver', methods=['POST'])
def transformed_insert_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Transformed Data received successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

@app.route('/TRANSFORMED_update_receiver', methods=['POST'])
def transformed_update_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Transformed Data updated successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

@app.route('/TRANSFORMED_delete_receiver', methods=['POST'])
def transformed_delete_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Transformed Data deleted successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

if __name__ == '__main__':
    load_dotenv()
    app.run(port=5000)
