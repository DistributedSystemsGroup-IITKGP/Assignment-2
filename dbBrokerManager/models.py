from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from db.config import Base
import datetime
from sqlalchemy.orm import relationship

class Logs(Base):
	__tablename__ = "Logs"

	log_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, nullable=False)
	topic_name = Column(String(255), unique=True, nullable=False)
	query_type = Column(String(255) ,nullable=False)
	producer_id = Column(Integer, default = -1 ,nullable=True)
	partition_id = Column(Integer, default = -1 ,nullable=True)
	consumer_id = Column(Integer, default = -1 ,nullable=True)
	log_msg = Column(String(255), nullable=True)
	status = Column(Integer, default = 0 , nullable=False)
	time_stamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

class Topics(Base):
	__tablename__ = "Topics"

	topic_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_name = Column(String(255), unique=True, nullable=False)
	last_partition = Column(Integer, default = 0 ,nullable=False)

class Consumers(Base):
	__tablename__ = "Consumers"

	consumer_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, nullable=False)

class Producers(Base):
	__tablename__ = "Producers"

	producer_id = Column(Integer, primary_key=True, autoincrement=True)
	topic_id = Column(Integer, nullable=False)