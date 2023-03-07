import time
from flask import Blueprint, jsonify, request
from dbBroker.config import async_session, engine, Base
from dbBroker.AsyncDAL import DAL

import datetime

server = Blueprint("server",__name__)

@server.before_app_first_request
async def setup_db():
	async with engine.begin() as conn:
		# await conn.run_sync(Base.metadata.drop_all)
		await conn.run_sync(Base.metadata.create_all, checkfirst=True)
		global topic_lock 
		global log_lock 
		topic_lock, log_lock = True, True

@server.route("/")
def index():
	return "<h1>Welcome to the In-memory Broker!</h1>"


@server.route("/status", methods=['GET'])
def status():
	return jsonify({"status": "success", "message": "broker running"})


@server.route("/topics", methods=["POST"])
async def create_topic():
	topic_name = request.json["topic_name"]
	partition_id = request.json["partition_id"]
	
	async with async_session() as session, session.begin():
		db_dal = DAL(session)
		return await db_dal.create_topic(topic_name, partition_id)
	
@server.route("/producer/produce", methods=["POST"])
async def enqueue():
	topic_name = request.json["topic_name"]
	partition_id = request.json['partition_id']
	producer_id = request.json["producer_id"]
	log_message = request.json["log_message"]
	
	async with async_session() as session, session.begin():
		db_dal = DAL(session)
		return await db_dal.enqueue(topic_name, partition_id, producer_id, log_message)


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
	topic_name = request.json["topic_name"]
	partition_id = request.json["partition_id"]
	consumer_front = request.json["consumer_front"]

	async with async_session() as session, session.begin():
		db_dal = DAL(session)
		return await db_dal.dequeue(topic_name, partition_id, consumer_front)

@server.route("/size", methods=["GET"])
async def size():
	topic_name = request.json["topic_name"]
	partition_id = request.json["partition_id"]
	consumer_front = request.json["consumer_front"]
	
	async with async_session() as session, session.begin():
		db_dal = DAL(session)
		return await db_dal.size(topic_name, partition_id, consumer_front)
