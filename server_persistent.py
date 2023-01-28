from flask import Blueprint, jsonify, request
from db.config import async_session, engine, Base
from db.AsyncDAL import DAL

server_p = Blueprint("server_p",__name__)

@server_p.before_app_first_request
async def setup_db():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all, checkfirst=True)

@server_p.route("/")
def index():
    return "<h1>Welcome to the Persistent Distributed Server_p!</h1>"


@server_p.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.create_topic(topic_name)

@server_p.route("/topics", methods=["GET"])
async def list_topics():
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.list_topics()


@server_p.route("/consumer/register", methods=["POST"])
async def register_consumer():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.register_consumer(topic_name)

@server_p.route("/producer/register", methods=["POST"])
async def register_producer():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.register_producer(topic_name)
    

@server_p.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.enqueue(topic_name, producer_id, log_message)


@server_p.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.dequeue(topic_name, consumer_id)


@server_p.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.size(topic_name, consumer_id)