from flask import Blueprint, jsonify, request
from db.config import async_session, engine, Base
from db.AsyncDAL import DAL

server = Blueprint("server",__name__)

@server.before_app_first_request
async def setup_db():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all, checkfirst=True)

@server.route("/")
def index():
    return "<h1>Welcome to the Persistent Distributed server!</h1>"


@server.route("/topics", methods=["POST"])
async def create_topic():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.create_topic(topic_name)

@server.route("/topics", methods=["GET"])
async def list_topics():
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.list_topics()


@server.route("/consumer/register", methods=["POST"])
async def register_consumer():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.register_consumer(topic_name)

@server.route("/producer/register", methods=["POST"])
async def register_producer():
    topic_name = request.json["topic_name"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.register_producer(topic_name)
    

@server.route("/producer/produce", methods=["POST"])
async def enqueue():
    topic_name = request.json["topic_name"]
    producer_id = request.json["producer_id"]
    log_message = request.json["log_message"]
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.enqueue(topic_name, producer_id, log_message)


@server.route("/consumer/consume", methods=["GET"])
async def dequeue():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]
    
    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.dequeue(topic_name, consumer_id)


@server.route("/size", methods=["GET"])
async def size():
    topic_name = request.json["topic_name"]
    consumer_id = request.json["consumer_id"]

    async with async_session() as session, session.begin():
        db_dal = DAL(session)
        return await db_dal.size(topic_name, consumer_id)