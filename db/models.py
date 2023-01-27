from sqlalchemy import Column, Integer, String
from db.config import Base
from sqlalchemy.orm import relationship

# available user roles: Admin, Mentor, Mentee
class Topic(Base):
	__tablename__ = "Topic"

	topic_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_name = Column(String(255), unique=True, nullable=False)
	producer_count = Column(Integer, default = 0 ,nullable=False)
	consumer_count = Column(Integer, default = 0 ,nullable=False)
	msg_count = Column(Integer, default = 0 ,nullable=False)
	producer = relationship("Producer")
	consumer = relationship("Consumer")
	log = relationship("Log")


class Consumer(Base):
	__tablename__ = "Consumer"

	consumer_id = Column(String(30), primary_key=True)
	topic_id = Column(Integer, ForeignKey("Topic"))
	front = Column(Integer, default=0, nullable=False)

class Producer(Base):
	__tablename__ = "Producer"

	producer_id = Column(String(30), primary_key=True)
	topic_id = Column(Integer, ForeignKey("Topic"))
	log = relationship("Log")

class Log(Base):
	__tablename__ = "Log"

	log_id = Column(String(30), primary_key=True)
	topic_id = Column(Integer, ForeignKey("Topic"))
	producer_id = Column(Integer, ForeignKey("Producer"))
	log_msg = Column(String(255), nullable=False)
	timestamp = Column(String(255), nullable=False)
