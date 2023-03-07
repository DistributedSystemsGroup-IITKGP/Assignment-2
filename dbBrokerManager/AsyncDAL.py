import os
import time

from flask import Blueprint, jsonify, request
from typing import List, Optional
from sqlalchemy import update, insert, text
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from dbBrokerManager.models import Topic, Consumer, Producer, Log, Partition, ConsumerFront
from importlib.machinery import SourceFileLoader

class DAL():
	def __init__(self, db_session: Session):
		self.db_session = db_session

	async def complete_backup(self):
		topicLastPartitionId = {}
		topicsDict = {}
		consumerTopic = {}
		producerTopic = {}

		try:
			query = await self.db_session.execute(select(Topic))
			topics = query.scalars().all()

			query = await self.db_session.execute(select(Consumer))
			consumers = query.scalars().all()

			query = await self.db_session.execute(select(Producer))
			producers = query.scalars().all()
			
			for topic in topics:
				topicsDict[topic.topic_name] = topic.topic_id
				topicLastPartitionId[topic.topic_name] = topic.last_partition
			
			for consumer in consumers:
				consumerTopic[consumer.consumer_id] = consumer.topic_id
			
			for producer in producers:
				producerTopic[producer.producer_id] = producer.topic_id

		finally:
			return topicsDict, topicLastPartitionId, consumerTopic, producerTopic


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
		new_topic = Topic(topic_name = topic_name, last_partition = 0)
		self.db_session.add(new_topic)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})

	async def add_partition_topic(self, topic_id, partition_id):
		new_partition = Partition(topic_id = topic_id, partition_id = partition_id)
		self.db_session.add(new_partition)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Partition '{partition_id}' added to topic '{topic_id}' successfully"})

	async def register_consumer(self, topic_id: int):
		new_consumer = Consumer(topic_id = topic_id)
		self.db_session.add(new_consumer)

		partitions = await self.db_session.execute(select(Partition).where(Partition.topic_id == topic_id))
		partitions = partitions.scalars().all()
		for partition_id in partitions:
			new_consumer_front = ConsumerFront(consumer_id = new_consumer.consumer_id, partition_id = partition_id.partition_id, topic_id = topic_id)
			self.db_session.add(new_consumer_front)

		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Consumer '{new_consumer.consumer_id}' registered successfully", "consumer_id": new_consumer.consumer_id})

	async def get_partitions(self, topic_id: int):
		partitions = await self.db_session.execute(select(Partition).where(Partition.topic_id == topic_id))
		partitions_list = []
		partitions = partitions.scalars().all()
		for partition in partitions:
			partitions_list.append(partition.partition_id)

		await self.db_session.flush()

		return jsonify({"status": "success", "partitions": partitions_list})

	async def get_consumer_front(self, consumer_id: int, topic_id: int, partition_id: int):
		consumer_front = await self.db_session.execute(select(ConsumerFront).where(ConsumerFront.consumer_id == consumer_id).where(ConsumerFront.topic_id == topic_id).where(ConsumerFront.partition_id == partition_id))
		consumer_front = consumer_front.scalars().all()
		await self.db_session.flush()

		return consumer_front

	async def update_consumer_front(self, consumer_id: int, topic_id: int, partition_id: int):
		query = await self.db_session.execute(update(ConsumerFront).where(ConsumerFront.consumer_id == consumer_id).where(ConsumerFront.partition_id == partition_id).where(ConsumerFront.topic_id==topic_id).values(front = ConsumerFront.front + 1))
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Consumer front updated successfully"})

	async def register_producer(self, topic_id: int):
		new_producer = Producer(topic_id = topic_id)
		self.db_session.add(new_producer)
		await self.db_session.flush()

		return jsonify({"status": "success", "message": f"Producer '{new_producer.producer_id}' registered successfully", "producer_id": new_producer.producer_id})