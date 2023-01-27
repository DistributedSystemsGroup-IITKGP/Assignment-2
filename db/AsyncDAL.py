from typing import List, Optional

from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import Session

from db.models import Topic

class DAL():
	def __init__(self, db_session: Session):
		self.db_session = db_session

	async def create_topic(self, topic_name: str):
		topic = await self.db_session.execute(select(Book).where(topic_name = topic_name))
		if topic_name:
			return jsonify({"status": "failure", "message": f"Topic '{topic_name}' already exists"})
		
		new_topic = Topic(topic_name = topic_name)
		# async with async_session() as session, session.begin():
		self.db_ession.add(new_topic)
		await self.db_session.flush()
		
		return jsonify({"status": "success", "message": f"Topic '{topic_name}' created successfully"})