from flask import Blueprint, jsonify, request
from typing import List, Optional

from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import Session

from db.models import Topic

class DAL():
	def __init__(self, db_session: Session):
		self.db_session = db_session

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
		topics = query.scalar()
    	return jsonify({"status": "success", "topics": list(topics.keys())})