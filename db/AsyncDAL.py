import os
import time

from flask import Blueprint, jsonify, request
from typing import List, Optional
from sqlalchemy import update, insert, text
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from db.models import Topic, Consumer, Producer, Log
from importlib.machinery import SourceFileLoader

class DAL():
	def __init__(self, db_session: Session):
		self.db_session = db_session

	async def complete_backup(self):
		path = os.path.abspath('..') + '/Distributed-Queue/log_queue.py'
		inMemoryQueue = SourceFileLoader("log_queue",path).load_module()
		logQueue = inMemoryQueue.InMemoryLogQueue()

		producersDict = {}
		topicsDict = {}
		topicNamesDict = {}
		consumersDict = {}

		try:
			query = await self.db_session.execute(select(Topic))
			topics = query.scalars().all()

			queryConsumers = await self.db_session.execute(select(Consumer))
			consumers = queryConsumers.scalars().all()

			queryLogs = await self.db_session.execute(select(Log))
			logs = queryLogs.scalars().all()

			queryProducers = await self.db_session.execute(select(Producer))
			producers = queryProducers.scalars().all()
			
			for topic in topics:
				logQueue.create_topic(topic.topic_name)
				topicsDict[topic.topic_name] = topic.topic_id
				topicNamesDict[topic.topic_id] = topic.topic_name

			for consumer in consumers:
				logQueue.register_consumer(consumer.consumer_id, consumer.front)
				consumersDict[consumer.consumer_id] = topicNamesDict[consumer.topic_id]
			
			for log in logs:
				logQueue.enqueue(topicNamesDict[log.topic_id], log.log_msg)

			for producer in producers:
				logQueue.register_producer(producer.producer_id, topic.topic_name)
				producersDict[producer.producer_id] = topicNamesDict[producer.topic_id]

		finally:
			return logQueue, topicsDict, consumersDict, producersDict

	async def create_topic(self, topic_name: str):
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()
		if topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
		
		new_topic = Topic(topic_name = topic_name)
		self.db_session.add(new_topic)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})

	async def list_topics(self):
		query = await self.db_session.execute(select(Topic))
		topics = query.scalars().all()
		topic_key = []
		for topic in topics:
			topic_key.append(topic.topic_name)
		return jsonify({"status": "success", "topics": topic_key})

	async def register_consumer(self, topic_name: str):
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()
		if not topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
		
		new_consumer = Consumer(topic_id = topic.topic_id)
		self.db_session.add(new_consumer)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "consumer_id": new_consumer.consumer_id})

	async def register_producer(self, topic_name: str):
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()

		if not topic:
			topic = Topic(topic_name = topic_name)
			try:
				self.db_session.add(topic)
				await self.db_session.flush()
			except:
				query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
				topic = query.scalar()

		
		new_producer = Producer(topic_id = topic.topic_id)
		self.db_session.add(new_producer)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "producer_id": new_producer.producer_id})

	async def enqueue(self, topic_name: str, producer_id: int, log_message: str):
		# if topic not present
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()
		if not topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
		
		# if producer not present or not registered to the topic
		query = await self.db_session.execute(select(Producer).filter_by(producer_id = producer_id))
		producer = query.scalar()
		if not producer or producer.topic_id != topic.topic_id:
	   		return jsonify({"status": "failure", "message": "Invalid producer_id"})
	
		# update metadata and add
		topic.msg_count += 1
		log_id = 'T'+str(topic.topic_id)+'#C'+str(topic.msg_count)
		new_log = Log(log_id = log_id,topic_id = topic.topic_id, producer_id = producer_id, timestamp = str(time.time()), log_msg = log_message)
		self.db_session.add(new_log)

		# commit the changes
		await self.db_session.flush()
		return jsonify({"status": "success"})


	async def dequeue(self, topic_name: str, consumer_id: int):
		# if topic not present
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()
		if not topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
		
		# if consumer not present or not registered to the topic
		query = await self.db_session.execute(select(Consumer).filter_by(consumer_id = consumer_id))
		consumer = query.scalar()
		if not consumer or consumer.topic_id != topic.topic_id:
	   		return jsonify({"status": "failure", "message": "Invalid consumer_id"})

		# if there is no new log to retrieve
		if topic.msg_count <= consumer.front:
			return jsonify({"status": "failure", "message": "Queue is empty"})
	
		consumer.front += 1
		log_id = 'T'+str(topic.topic_id)+'#C'+str(consumer.front)
		query = await self.db_session.execute(select(Log).filter_by(log_id = log_id))
		log = query.scalar()

		await self.db_session.flush()

		return jsonify({"status": "success", "log_message": log.log_msg})


	async def size(self, topic_name: str, consumer_id: int):
		# if topic not present
		query = await self.db_session.execute(select(Topic).filter_by(topic_name = topic_name))
		topic = query.scalar()
		if not topic:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' does not exist"})
		
		# if consumer not present or not registered to the topic
		query = await self.db_session.execute(select(Consumer).filter_by(consumer_id = consumer_id))
		consumer = query.scalar()
		if not consumer or consumer.topic_id != topic.topic_id:
	   		return jsonify({"status": "failure", "message": "Invalid consumer_id"})
		
		query = await self.db_session.execute(select(Log).filter_by(topic_id = topic.topic_id))
		count = len(query.scalars().all())
		size = count - consumer.front
		return jsonify({"status": "success", "size": size})
	