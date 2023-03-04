import time
from flask import Blueprint, jsonify, request
import requests
import part2.health_check as health_check

server = Blueprint("broker_manager_Read_Only", __name__)

brokers = list()                # List of brokers in the network
brokersReplica = list()         # List of replica of brokers in the network in same order as broker

topics_lock = True

class Broker:
    def __init__(self, address, broker_id, isReadOnly = False):
        self.address = address
        self.isAlive = True
        self.brokerID = broker_id
        self.isReadOnly = isReadOnly

    def declareDead(self):
        self.isAlive = False

    def declareAlive(self):
        self.isAlive = True
    
    async def checkHealth(self):
        try:
            r = await requests.get(f"{self.address}/status")
            response = r.json()
            if response["status"] == "success" and response["message"] == "broker running":
                self.declareAlive()
            else:
                self.declareDead()
        except:
            self.declareDead()

async def getServerAddress(broker_id):
    global topics_lock
    while topics_lock == False:
        time.sleep(1)
    topics_lock = False 

    address = None
        
    brokers[broker_id].checkHealth()
    if brokers[broker_id].isAlive:
        address = brokers[broker_id].address
    else:
        brokers[broker_id].declareDead()
    
    if address is None:
        for replica in brokersReplica[broker_id]:
            replica.checkHealth()
            if replica.isAlive:
                address = replica.address
            else:
                replica.declareDead()

    topics_lock = True

    return address

@server.before_app_first_request
async def setUpBrokerManager():
    # This function sets up the broker manager by backing up things from server and setting up brokers in the network and read only copies of broker manager.
    global brokers, brokersReplica, topics_lock
    topics_lock = True
    brokers = list()
    brokersReplica = list()

    ip, ports, portsReplica = health_check.doSearchJob(0)

    numBrokers = max([port[1] for port in ports])+1

    for broker in range(numBrokers):
        portsReplica[broker] = []
        port = None
        for portT in ports:
            if portT[1] == broker:
                port = portT
                break
        brokers.append(Broker(f"http://{ip}:{port[0]}", port[1]))
    
    for broker in range(numBrokers):
        for portT in portsReplica:
            if portT[1] == broker:
                brokersReplica[broker].append([Broker(f"http://{ip}:{port[0]}", port[1], True)])
    


@server.route("/")
def index():
    return "<h1>Welcome to the Broker Manager!</h1>"


@server.route("/status")
def status():
    return jsonify({"status": "success", "message": "Broker Manager Copy running"})
    

@server.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        params = {"topic_name": topic_name}
        r = requests.post(f"{address}/topics", json=params)
        response = r.json()
        if response["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Topic '{topic_name}' could not be created"})
    
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})


@server.route("/consumer/register", methods=["POST"])
async def register_consumer():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        params = {"topic_name": topic_name, "consumer_id": consumer_id}
        r = requests.post(f"{address}/consumer/register", json=params)
        response = r.json()
        if response["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Consumer '{consumer_id}' could not be registered"})
    
    return jsonify({"status": "success", "consumer_id": consumer_id})


@server.route("/producer/register", methods=["POST"])
async def register_producer():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]

    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        params = {"topic_name": topic_name, "producer_id": producer_id}
        r = requests.post(f"{address}/producer/register", json=params)
        response = r.json()
        if response["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Producer '{producer_id}' could not be registered"})

    return jsonify({"status": "success", "producer_id": producer_id})
    

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    partition_id = request.json["partition_id"]

    partition_id = partition_id % len(brokers)
    
    address = await getServerAddress(partition_id)
    if address is None:
        return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

    params = {"topic_name": topic_name, "producer_id": producer_id, "log_message": log_message}
    r = requests.post(f"{address}/producer/produce", json=params)
    response = r.json()
    if response["status"] == "failure":
        return jsonify({"status": "failure", "message": f"Message production failed"})

    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    log_message = None
    minTime = time.time()
    minbrokerID = None
    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        query = None
        params = {"topic_name": topic_name, "consumer_id": consumer_id}
        r = requests.get(f"{address}/consumer/consume_query", json=params)
        query = r.json()
        if query["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Message consumption failed"})
        
        if minTime < query["time_stamp"]:
            minTime = query["time_stamp"]
            log_message = query["log_message"]
            minbrokerID = broker_id
    
    if log_message is None:
        return jsonify({"status": "failure", "message": f"No message to consume"})

    address = await getServerAddress(minbrokerID)
    if address is None:
        return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})  
    params = {"topic_name": topic_name, "consumer_id": consumer_id}
    r = requests.get(f"{address}/consumer/consume", json=params)
    response = r.json()
    if response["status"] == "failure":
        return jsonify({"status": "failure", "message": f"Message consumption failed"})

    return jsonify({"status": "success", "log_message": log_message})


@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    consumer_size = 0

    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        query = None
        params = {"topic_name": topic_name, "consumer_id": consumer_id}
        r = requests.get(f"{address}/size", json=params)
        query = r.json()
        if query["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Size query failed"})
        consumer_size += query["size"]
    
    return jsonify({"status": "success", "size": consumer_size})