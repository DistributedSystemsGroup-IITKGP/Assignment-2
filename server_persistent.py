from flask import Blueprint, jsonify, request
# from db.models import {
#     Consumer,
#     Producer,
#     Topic,
#     Log,
# }
from db.config import async_session, engine, Base
from db.AsyncDAL import DAL

server_p = Blueprint("server_p",__name__)

@server_p.before_app_first_request
async def setup_db():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

@server_p.route("/")
def index():
    return "<h1>Welcome to the Persistent Distributed Server_p!</h1>"


@server_p.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.create_topic(topic_name)
    

    # topic = Topic.query.filter_by(topic_name = topic_name)
    # if topic_name:
    #     return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
    
    # new_topic = Topic(topic_name = topic_name)
    # async with async_session() as session, session.begin():
    #     session.add(new_topic)
    #     await session.flush
    
    # return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})


# @server_p.route("/topics", methods=["GET"])
# def list_topics():
#     return jsonify({"status": "success", "topics": list(topics.keys())})


# @server_p.route("/consumer/register", methods=["POST"])
# def register_consumer():
#     topic_name = request.json["topic_name"]
    
#     if topic_name not in topics:
#         return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
#     if topic_name not in topic_consumers:
#         topic_consumers[topic_name] = []

#     consumer_id = 'C_T' + str(topics[topic_name]) + '#' + str(len(topic_consumers[topic_name]))

#     log_queue.register_consumer(consumer_id)

#     topic_consumers[topic_name].append(consumer_id)
#     consumers[consumer_id] = topic_name
    
#     return jsonify({"status": "success", "consumer_id": consumer_id})


# @server_p.route("/producer/register", methods=["POST"])
# def register_producer():
#     topic_name = request.json["topic_name"]
    
#     if topic_name not in topic_producers:
#         topic_producers[topic_name] = []
#         # log_queue[topic_name] = []
#         log_queue.create_topic(topic_name)
    
#     producer_id = 'P_T' + str(topics[topic_name]) + '#' + str(len(topic_producers[topic_name]))
#     topic_producers[topic_name].append(producer_id)
#     producers[producer_id] = topic_name
    
#     return jsonify({"status": "success", "producer_id": producer_id})


# @server_p.route("/producer/produce", methods=["POST"])
# def enqueue():
#     topic_name = request.json["topic_name"]
#     producer_id = request.json["producer_id"]
#     log_message = request.json["log_message"]
    
#     if topic_name not in topic_producers:
#         return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
#     if producer_id not in producers or producers[producer_id] != topic_name:
#         return jsonify({"status": "failure", "message": "Invalid producer_id"})
    
#     # log_queue[topic_name].append(log_message)
#     log_queue.enqueue(topic_name, log_message)
    
#     return jsonify({"status": "success"})


# @server_p.route("/consumer/consume", methods=["GET"])
# def dequeue():
#     topic_name = request.json["topic_name"]
#     consumer_id = request.json["consumer_id"]
    
#     if topic_name not in topic_consumers:
#         return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
#     if consumer_id not in consumers or consumers[consumer_id] != topic_name:
#         return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
#     if log_queue.empty(topic_name, consumer_id):
#         return jsonify({"status": "failure", "message": "Queue is empty"})
    
#     # log_message = log_queue[topic_name][consumers_front[consumer_id]]
#     # consumers_front[consumer_id] += 1
#     log_message = log_queue.dequeue(topic_name, consumer_id)

#     return jsonify({"status": "success", "log_message": log_message})


# @server_p.route("/size", methods=["GET"])
# def size():
#     topic_name = request.json["topic_name"]
#     consumer_id = request.json["consumer_id"]

#     if topic_name not in topics:
#         return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
    
#     if consumer_id not in consumers or consumers[consumer_id] != topic_name:
#         return jsonify({"status": "failure", "message": "Invalid consumer_id"})
    
#     return jsonify({"status": "success", "size": log_queue.size(topic_name, consumer_id)})

