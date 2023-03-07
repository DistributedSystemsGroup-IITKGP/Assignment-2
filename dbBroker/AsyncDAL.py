import datetime

from flask import jsonify
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from dbBroker.models import Topic, LogMessage

class DAL():
	def __init__(self, db_session: Session):
		self.db_session = db_session

	async def create_topic(self, topic_name: str, partition_id: int):
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name, partition_id = partition_id))
		topic = query.scalar()
		if topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' partition '{partition_id}' already exists"})
		
		qname = topic_name+'#'+str(partition_id)
		new_topic = Topic(topic_id = qname, topic_name = topic_name, partition_id = partition_id)
		self.db_session.add(new_topic)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "message": f"Topic '{topic_name}' partition '{partition_id}' created successfully"})

	async def enqueue(self, topic_name: str, partition_id: int, producer_id: int, log_message: str):
		timestamp = datetime.datetime.utcnow()
		qname = topic_name+'#'+str(partition_id)
		
		# if topic not present
		query = await self.db_session.execute(select(Topic).filter_by(topic_id = qname))
		topic = query.scalar()
		
		# update metadata and add
		topic.count_msg += 1
		log_id = qname+'#'+str(topic.count_msg)
		new_log = LogMessage(log_id = log_id, topic_id = topic.topic_id, producer_id = producer_id, time_stamp = timestamp, log_msg = log_message)
		self.db_session.add(new_log)

		# commit the changes
		await self.db_session.flush()
		
		return jsonify({"status": "success"})

	async def dequeue(self, topic_name: str, partition_id: int, consumer_front: int):
		qname = topic_name+'#'+str(partition_id)
		query = await self.db_session.execute(select(Topic).filter_by(topic_id = qname))
		topic = query.scalar()
		
		# if there is no new log to retrieve
		if topic.count_msg <= consumer_front:
			return jsonify({"status": "failure", "message": "Queue is empty"})
	
		log_id = qname+'#'+str(consumer_front+1)
		query = await self.db_session.execute(select(LogMessage).filter_by(log_id = log_id))
		log = query.scalar()


		return jsonify({"status": "success", "log_message": log.log_msg, 'timestamp': log.time_stamp})

	
	async def size(self, topic_name: str, partition_id: int, consumer_front: int):
		qname = topic_name+'#'+str(partition_id)
		query = await self.db_session.execute(select(Topic).filter_by(topic_id = qname))
		topic = query.scalar()
		
		l = topic.count_msg - consumer_front
		return jsonify({"status": "success", "size": l})

	