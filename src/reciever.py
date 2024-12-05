from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = Flask(__name__)

@app.route('/RAW_insert_receiver', methods=['POST'])
def raw__insert_receive():
    data = request.json  
    print("Received JSON data:", data)
    print("Sending to Transformation...")
    response = {"status": "success", "message": "Raw Data received successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

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

@app.route('/TRANSFORMED_receiver', methods=['POST'])
def transformed_receive():
    data = request.json  
    print("Received JSON data:", data)
    response = {"status": "success", "message": "Transformed Data received successfully"}
    print("gonna send this info somewhere...")
    return jsonify(response), 200

if __name__ == '__main__':
    app.run(port=5000)
