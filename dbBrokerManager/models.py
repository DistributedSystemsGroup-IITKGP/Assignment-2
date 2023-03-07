from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from dbBrokerManager.config import BaseBroker as Base
import datetime
from sqlalchemy.orm import relationship

class Log(Base):
	__tablename__ = "Logs"

	log_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, nullable=False)
	topic_name = Column(String(255) , nullable=False)
	query_type = Column(String(255) ,nullable=False)
	producer_id = Column(Integer, default = -1 ,nullable=True)
	partition_id = Column(Integer, default = -1 ,nullable=True)
	consumer_id = Column(Integer, default = -1 ,nullable=True)
	log_msg = Column(String(255), nullable=True)
	status = Column(Integer, default = 0 , nullable=False)
	time_stamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

class Topic(Base):
	__tablename__ = "Topic"

	topic_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_name = Column(String(255), unique=True, nullable=False)
	last_partition = Column(Integer, default = 0 ,nullable=False)
	consumer = relationship("Consumer")
	producer = relationship("Producer")
	partition = relationship("Partition")
	consumerFront = relationship("ConsumerFront")

class Consumer(Base):
	__tablename__ = "Consumer"

	consumer_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, ForeignKey("Topic.topic_id", ondelete="CASCADE"), nullable=False)
	consumerFront = relationship("ConsumerFront")

class Producer(Base):
	__tablename__ = "Producer"

	producer_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, ForeignKey("Topic.topic_id", ondelete="CASCADE"), nullable=False)

class Partition(Base):
	__tablename__ = "Partition"
	topic_id = Column(Integer, ForeignKey("Topic.topic_id", ondelete="CASCADE"), nullable=False, primary_key=True)
	partition_id = Column(Integer, default = 0 ,nullable=False, primary_key=True)

	consumerFront = relationship("ConsumerFront")

class ConsumerFront(Base):
	__tablename__ = "ConsumerFront"

	consumer_id = Column(Integer, ForeignKey("Consumer.consumer_id", ondelete="CASCADE"), nullable=False, primary_key=True)
	topic_id = Column(Integer, ForeignKey("Topic.topic_id", ondelete="CASCADE"), nullable=False, primary_key=True)
	partition_id = Column(Integer, ForeignKey("Partition.partition_id", ondelete="CASCADE"), nullable=False, primary_key=True)
	front = Column(Integer, default=0, nullable=False)