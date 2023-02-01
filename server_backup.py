import time
from flask import Blueprint, jsonify, request
from db.config import async_session, engine, Base
from db.AsyncDAL import DAL
from log_queue import InMemoryLogQueue

server = Blueprint("server_backup",__name__)

log_queue = InMemoryLogQueue()
topics = {} # topic_name to topic_id
consumers = {} # consumer_id to topic_name mapping
producers = {} # producer_id to topic_name mapping
topic_lock = True
log_lock = True

@server.before_app_first_request
async def setup_db_and_backup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, checkfirst=True)
        global topic_lock 
        global log_lock 
        topic_lock, log_lock = True, True
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        global log_queue
        global topics
        global consumers
        global producers
        log_queue, topics, consumers, producers =  await db_dal.complete_backup()

@server.route("/")
def index():
    return "<h1>Welcome to the Backed-up Distributed server!</h1>"


@server.route("/status")
def status():
    return jsonify({"status": "success"})
    

@server.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    if topic_name in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
    
    topics[topic_name] = len(topics)
    log_queue.create_topic(topic_name)


    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.create_topic(topic_name)
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})
    

@server.route("/topics", methods=["GET"])
async def list_topics():
    return jsonify({"status": "success", "topics": list(topics.keys())})


@server.route("/consumer/register", methods=["POST"])
async def register_consumer():
    topic_name = request.json["topic_name"]
    
    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})

    consumer_id = len(consumers)+1
    log_queue.register_consumer(consumer_id)
    consumers[consumer_id] = topic_name

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.register_consumer(topic_name)
    return jsonify({"status": "success", "consumer_id": consumer_id})

@server.route("/producer/register", methods=["POST"])
async def register_producer():
    topic_name = request.json["topic_name"]

    global topic_lock
    while not topic_lock:
        time.sleep(1)
    topic_lock = False
    if topic_name not in topics:
        topics[topic_name] = len(topics)
        log_queue.create_topic(topic_name)
    
    producer_id = len(producers)+1
    producers[producer_id] = topic_name

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.register_producer(topic_name)
        topic_lock = True
    return jsonify({"status": "success", "producer_id": producer_id})
    

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    
    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    if producer_id not in producers or producers[producer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid producer_id"})
    
    global log_lock
    while not log_lock:
        time.sleep(1)
    log_lock = False
    log_queue.enqueue(topic_name, log_message, )
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.enqueue(topic_name, producer_id, log_message)
        log_lock = True
    return jsonify({"status": "success"})

@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if consumer_id not in consumers or consumers[consumer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
    if log_queue.empty(topic_name, consumer_id):
        return jsonify({"status": "failure", "message": "Queue is empty"})
    
    global log_lock
    while not log_lock:
        time.sleep(1)
    log_lock = False
    log_message = log_queue.dequeue(topic_name, consumer_id)
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.dequeue(topic_name, consumer_id)
        log_lock = True

    return jsonify({"status": "success", "log_message": log_message})

@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if consumer_id not in consumers or consumers[consumer_id] != topic_name:
        return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
    return jsonify({"status": "success", "size": log_queue.size(topic_name, consumer_id)})