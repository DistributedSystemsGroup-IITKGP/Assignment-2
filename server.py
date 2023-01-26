from flask import Blueprint, jsonify, request
from log_queue import InMemoryLogQueue

server = Blueprint("server",__name__)

log_queue = InMemoryLogQueue()

topics = {} # topic_name to topic_id mapping
consumers = {} # Keep track of registered consumers
topic_consumers = {} # topic to consumer_id mapping
producers = {} # Keep track of registered producers
topic_producers = {} # topic to producer_id mapping


@server.route("/")
def index():
    return "<h1>Welcome to the Distributed Server!</h1>"


@server.route("/topics", methods=["POST"])
def create_topic():
    topic_name = request.json["topic_name"]
    
    if topic_name in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
    
    topics[topic_name] = len(topics)
    # log_queue[topic_name] = []
    log_queue.create_topic(topic_name)
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})


@server.route("/topics", methods=["GET"])
def list_topics():
    return jsonify({"status": "success", "topics": list(topics.keys())})


@server.route("/consumer/register", methods=["POST"])
def register_consumer():
    topic_name = request.json["topic_name"]
    
    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if topic_name not in topic_consumers:
        topic_consumers[topic_name] = []

    consumer_id = 'C_T' + str(topics[topic_name]) + '#' + str(len(topic_consumers[topic_name]))

    log_queue.register_consumer(consumer_id)

    topic_consumers[topic_name].append(consumer_id)
    consumers[consumer_id] = topic_name
    
    return jsonify({"status": "success", "consumer_id": consumer_id})


@server.route("/producer/register", methods=["POST"])
def register_producer():
    topic_name = request.json["topic_name"]
    
    if topic_name not in topic_producers:
        topic_producers[topic_name] = []
        # log_queue[topic_name] = []
        log_queue.create_topic(topic_name)
    
    producer_id = 'P_T' + str(topics[topic_name]) + '#' + str(len(topic_producers[topic_name]))
    topic_producers[topic_name].append(producer_id)
    producers[producer_id] = topic_name
    
    return jsonify({"status": "success", "producer_id": producer_id})


@server.route("/producer/produce", methods=["POST"])
def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    
    if topic_name not in topic_producers:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if producer_id not in producers or producers[producer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid producer_id"})
    
    # log_queue[topic_name].append(log_message)
    log_queue.enqueue(topic_name, log_message)
    
    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    if topic_name not in topic_consumers:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if consumer_id not in consumers or consumers[consumer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
    if log_queue.empty(topic_name, consumer_id):
        return jsonify({"status": "failure", "message": "Queue is empty"})
    
    # log_message = log_queue[topic_name][consumers_front[consumer_id]]
    # consumers_front[consumer_id] += 1
    log_message = log_queue.dequeue(topic_name, consumer_id)

    return jsonify({"status": "success", "log_message": log_message})


@server.route("/size", methods=["GET"])
def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if consumer_id not in consumers or consumers[consumer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
    return jsonify({"status": "success", "size": log_queue.size(topic_name, consumer_id)})

