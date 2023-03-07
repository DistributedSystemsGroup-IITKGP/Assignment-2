import time
from flask import Blueprint, jsonify, request
import requests
import part2.health_check as health_check
from dbBrokerManager.config import async_session, engine, BaseBroker
from dbBrokerManager.AsyncDAL import DAL
import datetime

server = Blueprint("broker_manager_Read_Only", __name__)

brokers = list()                  # List of brokers in the network


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
            r = requests.get(f"{self.address}/status")
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
        
    await brokers[broker_id].checkHealth()
    if brokers[broker_id].isAlive:
        address = brokers[broker_id].address
    else:
        brokers[broker_id].declareDead()
    
    # if address is None:
    #     for replica in brokersReplica[broker_id]:
    #         replica.checkHealth()
    #         if replica.isAlive:
    #             address = replica.address
    #         else:
    #             replica.declareDead()

    topics_lock = True

    return address

@server.before_app_first_request
async def setUpBrokerManager():
    # This function sets up the broker manager by backing up things from server and setting up brokers in the network and read only copies of broker manager.
    global brokers, topics_lock
    topics_lock = True
    brokers = list()

    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(BaseBroker.metadata.create_all, checkfirst=True)

    ip, ports = health_check.doSearchJob(0)
    print(ports)
    for id, port in enumerate(ports):
        brokers.append(Broker(f"http://{ip}:{port}", id))


@server.route("/")
def index():
    return "<h1>Welcome to the Broker Manager Copy!</h1>"


@server.route("/status")
def status():
    return jsonify({"status": "success", "message": "Broker Manager Copy running"})
    

@server.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    brokers_list = list()

    for broker_id in range(len(brokers)):
        address = await getServerAddress(broker_id)
        print(address)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        params = {"topic_name": topic_name, "partition_id": broker_id}
        r = requests.post(f"{address}/topics", json=params)
        response = r.json()
        if response["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Topic '{topic_name}' could not be created"})
        brokers_list.append(broker_id)
    return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully", "brokers_list": brokers_list})
   

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    partition_id = request.json["partition_id"]
    
    address = await getServerAddress(partition_id)
    if address is None:
        return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

    params = {"topic_name": topic_name, "producer_id": producer_id, "log_message": log_message, "partition_id": partition_id}
    r = requests.post(f"{address}/producer/produce", json=params)
    response = r.json()
    if response["status"] == "failure":
        return jsonify({"status": "failure", "message": f"Message production failed"})

    return jsonify({"status": "success"})


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    partitions = request.json["partitions"]
    consumerFront = request.json["consumer_fronts"]

    log_message = None
    minTime = datetime.datetime.utcnow()
    minbrokerID = None

    for broker_id in partitions:
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        query = None
        params = {"topic_name": topic_name, "partition_id": broker_id, "consumer_front": consumerFront[str(broker_id)]}
        r = requests.get(f"{address}/consumer/consume", json=params)
        query = r.json()
        
        if query["status"] == "failure":
            continue

        if minTime > datetime.datetime.strptime(query["timestamp"], '%a, %d %b %Y %H:%M:%S %Z'):
            minTime = datetime.datetime.strptime(query["timestamp"], '%a, %d %b %Y %H:%M:%S %Z')
            log_message = query["log_message"]
            minbrokerID = broker_id
    
    if log_message is None:
        return jsonify({"status": "failure", "message": f"No message to consume"})

    return jsonify({"status": "success", "log_message": log_message, "broker_id": minbrokerID})


@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    partitions = request.json["partitions"]
    consumerFront = request.json["consumer_fronts"]

    consumer_size = 0

    for broker_id in partitions:
        address = await getServerAddress(broker_id)
        if address is None:
            return jsonify({"status": "failure", "message": f"Data Lost due to complete broker failure"})

        query = None
        params = {"topic_name": topic_name, "partition_id": broker_id, "consumer_front": consumerFront[str(broker_id)]}
        r = requests.get(f"{address}/size", json=params)
        query = r.json()
        if query["status"] == "failure":
            return jsonify({"status": "failure", "message": f"Size query failed"})
        consumer_size += query["size"]
    
    return jsonify({"status": "success", "size": consumer_size})