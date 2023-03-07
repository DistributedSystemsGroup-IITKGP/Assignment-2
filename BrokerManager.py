import time
import requests
from flask import Blueprint, jsonify, request, url_for
from dbBrokerManager.config import async_session, engine, BaseBroker
from dbBrokerManager.AsyncDAL import DAL
import part2.health_check as health_check

server = Blueprint("broker_manager",__name__)

readOnlyCopies = list()         # Readonly copies of broker manager
topics = {}                     # topic_name to topic_id
consumerTopic = {}              # consumer_id to topic_id
producerTopic = {}              # producer_id to topic_id
topicLastPartitionId = dict()   # topic_name to last partition id
lastServerRequest = -1          # Last server which was requested

topics_lock = True

class ReadOnlyCopy:
    def __init__(self, address):
        self.address = address
        self.isAlive = True

    def declareDead(self):
        self.isAlive = False

    def declareAlive(self):
        self.isAlive = True
    
    def checkHealth(self):
        try:
            r = requests.get(f"{self.address}/status")
            response = r.json()
            if response["status"] == "success" and response["message"] == "Broker Manager Copy running":
                self.declareAlive()
            else:
                self.declareDead()
        except:
            self.declareDead()

async def getServerAddress():
    address = None

    global lastServerRequest

    while 1:
        while len(readOnlyCopies)==0:
            ip, ports = health_check.doSearchJob(1)
            for port in ports:
                readOnlyCopies.append(ReadOnlyCopy(f"http://{ip}:{port}"))
        
        lastServerRequest = (lastServerRequest + 1)%len(readOnlyCopies)
        if readOnlyCopies[lastServerRequest].isAlive:
            readOnlyCopies[lastServerRequest].checkHealth()
            if readOnlyCopies[lastServerRequest].isAlive:
                address = readOnlyCopies[lastServerRequest].address
                break
            else:
                readOnlyCopies.pop(lastServerRequest)
        else:
            readOnlyCopies.pop(lastServerRequest)
    return address

async def getMyAddress():
    ip, port = health_check.doSearchJob(2)
    return f"http://{ip}:{port[0]}"

@server.before_app_first_request
async def setUpBrokerManager():
    ip, ports = health_check.doSearchJob(1)
    print(ports)
    for port in ports:
        print("Registered Broker Manager Copy at port " + str(port))
        readOnlyCopies.append(ReadOnlyCopy(f"http://{ip}:{port}"))
    
    global topics, topicLastPartitionId, topics_lock, consumerTopic, producerTopic
    topics_lock = True

    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(BaseBroker.metadata.create_all, checkfirst=True)

    async with async_session() as session, session.begin():
        topics, topicLastPartitionId, consumerTopic, producerTopic = await DAL(session).complete_backup()

@server.route("/")
def index():
    return "<h1>Welcome to the Broker Manager!</h1>"


@server.route("/status")
def status():
    return jsonify({"status": "success", "message": "Broker Manager running"})
    

@server.route("/topics", methods=["POST"])
async def create_topic():
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    topic_name = request.json["topic_name"]

    if topic_name in topics:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.create_topic(topic_name)
        topics[topic_name] = len(topics)+1
        topicLastPartitionId[topic_name] = 0
        query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, None, "Create Topic", 0)

    params = {
        "topic_name" : topic_name
    }
    address = await getServerAddress()
    print(address)
    r = requests.post(url = address + '/topics', json = params)
    print(r)
    response = r.json()

    if response["status"] == "failure":
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' could not be created on the server"})

    print(response)

    brokers_list = response["brokers_list"]
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        for broker_id in brokers_list:
            await db_dal.add_partition_topic(topics[topic_name], broker_id)

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])

    topics_lock = True
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})
    

@server.route("/topics", methods=["GET"])
async def list_topics():
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    topics_list = list(topics.keys())

    topics_lock = True

    return jsonify({"status": "success", "topics": topics_list})


@server.route("/consumer/register", methods=["POST"])
async def register_consumer():
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    topic_name = request.json["topic_name"]
    consumer_id = None
    
    if topic_name not in topics:
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, None, "Register Consumer", 0)
        consumer_query = await db_dal.register_consumer(topics[topic_name])

    consumer_id = consumer_query.json["consumer_id"]
    consumerTopic[consumer_id] = topics[topic_name]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])

    topics_lock = True

    return jsonify({"status": "success", "consumer_id": consumer_id})


@server.route("/producer/register", methods=["POST"])
async def register_producer():
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    topic_name = request.json["topic_name"]
    producer_id = None

    if topic_name not in topics:
        async with async_session() as session, session.begin():
            db_dal = DAL(session)
            await db_dal.create_topic(topic_name)
            topics[topic_name] = len(topics)+1
            topicLastPartitionId[topic_name] = 0
            query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, None, "Create Topic", 0)

        params = {
            "topic_name" : topic_name
        }
        address = await getServerAddress()
        print(address)
        r = requests.post(url = address + '/topics', json = params)
        print(r.json())
        response = r.json()

        if response["status"] == "failure":
            topics_lock = True
            return jsonify({"status": "failure", "message": f"Topic '{topic_name}' could not be created on the server"})

        brokers_list = response["brokers_list"]
        async with async_session() as session, session.begin():
            db_dal = DAL(session)
            for broker_id in brokers_list:
                await db_dal.add_partition_topic(topics[topic_name], broker_id)

        async with async_session() as session, session.begin():
            db_dal = DAL(session)
            await db_dal.mark_complete(query.json["log_id"])
        
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, None, "Register Producer", 0)
        producer_query = await db_dal.register_producer(topics[topic_name])

    producer_id = producer_query.json["producer_id"]
    producerTopic[producer_id] = topics[topic_name]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])

    topics_lock = True

    return jsonify({"status": "success", "producer_id": producer_id})
    

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]

    if topic_name not in topics:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
    if producer_id not in producerTopic:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Producer '{producer_id}' does not exist"})

    if producerTopic[producer_id]!=topics[topic_name]:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Producer '{producer_id}' does not registered with '{topic_name}'"})

    if "partition_id" in request.json:
        partition_id = request.json["partition_id"]
    else:
        partition_id = topicLastPartitionId[topic_name] + 1
        topicLastPartitionId[topic_name] = partition_id

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        query = await db_dal.add_log(topic_name, topics[topic_name], producer_id, partition_id, log_message, None, "Produce", 0)
        partitions = await db_dal.get_partitions(topics[topic_name])
        partitions = partitions.json["partitions"]
    
    partition_id %= len(partitions)

    params = {
        "topic_name" : topic_name,
        "producer_id" : producer_id,
        "partition_id" : partition_id,
        "log_message" : log_message
    }
    address = await getServerAddress()
    r = requests.post(url = address + '/producer/produce', json = params)
    response = r.json()
    
    if response["status"]!="success":
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Failed to produce message to topic '{topic_name}'"})

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])

    topics_lock = True

    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False

    if topic_name not in topics:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})

    if consumer_id not in consumerTopic.keys():
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Consumer '{consumer_id}' does not exist"})

    if consumerTopic[consumer_id]!=topics[topic_name]:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Consumer '{consumer_id}' does not registered with '{topic_name}'"})
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, consumer_id, "Consume", 0)
        partitions = await db_dal.get_partitions(topics[topic_name])
        partitions = partitions.json["partitions"]

    consumerFronts = dict()
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        for partition_id in partitions:
            consumerFronts[partition_id] = await db_dal.get_consumer_front(consumer_id, topics[topic_name], partition_id)
            consumerFronts[partition_id] = consumerFronts[partition_id][0].front

    log_message = None
    params = {
        "topic_name" : topic_name,
        "consumer_id" : consumer_id,
        "partitions" : partitions,
        "consumer_fronts" : consumerFronts
    }

    address = await getServerAddress()
    r = requests.get(url = address + '/consumer/consume', json = params)
    response = r.json()
    if response["status"] == "success":
        log_message = response["log_message"]
        partition_updated = response["broker_id"]
        async with async_session() as session, session.begin():
            db_dal = DAL(session)
            await db_dal.update_consumer_front(consumer_id, topics[topic_name], partition_updated)
    else:
        topics_lock = True
        return jsonify({"status": "failure", "message": response["message"]})

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])
    
    topics_lock = True

    return jsonify({"status": "success", "log_message": log_message})


@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False

    if topic_name not in topics:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})

    if consumer_id not in consumerTopic.keys():
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Consumer '{consumer_id}' does not exist"})

    if consumerTopic[consumer_id]!=topics[topic_name]:
        topics_lock = True
        return jsonify({"status": "failure", "message": f"Consumer '{consumer_id}' does not registered with '{topic_name}'"})

    consumer_size = 0
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        query = await db_dal.add_log(topic_name, topics[topic_name], None, None, None, consumer_id, "Size", 0)
        partitions = await db_dal.get_partitions(topics[topic_name])
        partitions = partitions.json["partitions"]

    consumerFronts = dict()
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        for partition_id in partitions:
            consumerFronts[partition_id] = await db_dal.get_consumer_front(consumer_id, topics[topic_name], partition_id)
            consumerFronts[partition_id] = consumerFronts[partition_id][0].front

    params = {
        "topic_name" : topic_name,
        "consumer_id" : consumer_id,
        "partitions" : partitions,
        "consumer_fronts" : consumerFronts
    }
    address = await getServerAddress()
    r = requests.get(url = address + '/size', json = params)
    response = r.json()
    if response["status"] == "success":
        consumer_size = response["size"]
    else:
        topics_lock = True
        return jsonify({"status": "failure", "message": response["message"]})
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        await db_dal.mark_complete(query.json["log_id"])

    topics_lock = True

    return jsonify({"status": "success", "size": consumer_size})