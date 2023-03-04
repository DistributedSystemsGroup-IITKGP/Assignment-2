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
		topicLastPartitionId = {}
		topicsDict = {}

		try:
			query = await self.db_session.execute(select(Topic))
			topics = query.scalars().all()
			
			for topic in topics:
				topicsDict[topic.topic_name] = topic.topic_id
				topicLastPartitionId[topic.topic_name] = topic.last_partition

		finally:
			return topicsDict, topicLastPartitionId


	async def add_log(self, topic_name: str, topic_id: int, producer_id: int, partition_id: int, log_message: str, consumer_id: int, query_type: str, status: int):
		new_log = Log(topic_name = topic_name, topic_id = topic_id, producer_id = producer_id, partition_id = partition_id, log_msg = log_message, consumer_id = consumer_id, query_type = query_type, status = status)
		self.db_session.add(new_log)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "log_id": new_log.log_id})

	async def mark_complete(self, log_id: int):
		query = await self.db_session.execute(update(Log).where(Log.log_id == log_id).values(status = 1))
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Log marked complete for log_id '{log_id}'"})

	async def get_incomplete_logs(self):
		query = await self.db_session.execute(select(Log).where(Log.status == 0))
		logs = query.scalars().all()
		logs_list = []
		for log in logs:
			logs_list.append({"log_id": log.log_id, "topic_id": log.topic_id, "producer_id": log.producer_id, "log_msg": log.log_msg, "consumer_id": log.consumer_id, "query_type": log.query_type, "status": log.status})
		return jsonify({"status": "success", "logs": logs_list})

	async def list_topics(self):
		query = await self.db_session.execute(select(Topic))
		topics = query.scalars().all()
		topic_key = []
		for topic in topics:
			topic_key.append(topic.topic_name)
		return jsonify({"status": "success", "topics": topic_key})

	async def create_topic(self, topic_name: str):
		new_topic = Topic(topic_name = topic_name)
		self.db_session.add(new_topic)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})
	
	async def register_consumer(self, topic_id: str):
		new_consumer = Consumer(topic_id = topic_id)
		self.db_session.add(new_consumer)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Consumer '{new_consumer.consumer_id}' registered successfully", "consumer_id": new_consumer.consumer_id})

	async def register_producer(self, topic_id: str):
		new_producer = Producer(topic_id = topic_id)
		self.db_session.add(new_producer)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Producer '{new_producer.producer_id}' registered successfully", "producer_id": new_producer.producer_id})