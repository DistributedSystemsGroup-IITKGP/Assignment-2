from flask import Blueprint, jsonify, request
from part2.log_queue import InMemoryLogQueue
import datetime

server = Blueprint("server",__name__)

log_queue = InMemoryLogQueue()

topics = {} # topic_name to topic_id
partitions = {} # topic_name to partition_id
consumers = {} # consumer_id to topic_name mapping
producers = {} # producer_id to topic_name mapping


@server.route("/")
def index():
    return "<h1>Welcome to the In-memory Broker!</h1>"


@server.route("/status", methods=['GET'])
def status():
    return jsonify({"status": "success", "message": "broker running"})


@server.route("/topics", methods=["POST"])
def create_topic():
    topic_name = request.json["topic_name"]
    partition_id = request.json["partition_id"]
    
    if topic_name in topics and partition_id == partitions[topic_name]:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' partition '{partition_id}' already exists"})
    
    topics[topic_name] = len(topics)
    partitions[topic_name] = partition_id
    log_queue.create_topic(topic_name, partition_id)
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' partition '{partition_id}' created successfully"})


@server.route("/producer/produce", methods=["POST"])
def enqueue():
    topic_name = request.json["topic_name"]
    partition_id = request.json['partition_id']
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]

    timestamp = datetime.datetime.utcnow()

    log_queue.enqueue(topic_name, partition_id, producer_id, log_message, timestamp)
    
    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
def dequeue():
    topic_name = request.json["topic_name"]
    partition_id = request.json["partition_id"]
    consumer_front = request.json["consumer_front"]

    if log_queue.empty(topic_name, partition_id, consumer_front):
        return jsonify({"status": "failure", "message": "Queue is empty"})
    
    log_message, _, timestamp = log_queue.dequeue(topic_name, partition_id, consumer_front)

    return jsonify({"status": "success", "log_message": log_message, 'timestamp': timestamp})


@server.route("/size", methods=["GET"])
def size():
    topic_name = request.json["topic_name"]
    partition_id = request.json["partition_id"]
    consumer_front = request.json["consumer_front"]

    return jsonify({"status": "success", "size": log_queue.size(topic_name, partition_id, consumer_front)})