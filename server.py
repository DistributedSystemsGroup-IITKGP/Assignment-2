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

@app.route("/topics", methods=["GET"])
def list_topics():
    return jsonify({"status": "success", "topics": list(topics.keys())})


@app.route("/consumer/register", methods=["POST"])
def register_consumer():
    topic_name = request.json["topic_name"]
    
    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    consumer_id = len(topics[topic_name])
    topics[topic_name].append(consumer_id)
    consumers[consumer_id] = topic_name
    
    return jsonify({"status": "success", "consumer_id": consumer_id})


@app.route("/producer/register", methods=["POST"])
def register_producer():
    topic_name = request.json["topic_name"]
    
    if topic_name not in topics:
        topics[topic_name] = []
        log_queue[topic_name] = []
    
    producer_id = len(topics[topic_name])
    topics[topic_name].append(producer_id)
    producers[producer_id] = topic_name
    
    return jsonify({"status": "success", "producer_id": producer_id})

