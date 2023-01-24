from flask import Flask, jsonify, request
from queue import log_queue

app = Flask(__name__)

topics = {}
consumers = {}
producers = {}

@app.route("/")
def index():
    return "<h1>Welcome to the Distributed Server!</h1>"


@app.route("/topics", methods=["POST"])
def create_topic():
    topic_name = request.json["topic_name"]
    
    if topic_name in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
    
    topics[topic_name] = []
    log_queue[topic_name] = []
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})
