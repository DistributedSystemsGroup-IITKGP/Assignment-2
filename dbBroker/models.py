from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from dbBroker.config import Base
import datetime
from sqlalchemy.orm import relationship

class Topic(Base):
	__tablename__ = "Topic"

	topic_id = Column(String(255), primary_key=True)
	topic_name = Column(String(255), unique=True, nullable=False)
	partition_id = Column(Integer,nullable=False)
	count_msg = Column(Integer,default=0, nullable=False)
	topic = relationship("LogMessage")

class LogMessage(Base):
	__tablename__ = "LogMessage"

	log_id = Column(String(255), primary_key=True)
	topic_id = Column(String(255), ForeignKey("Topic.topic_id", ondelete="CASCADE"), nullable=False)
	producer_id = Column(Integer, default = -1 ,nullable=True)
	log_msg = Column(String(255), nullable=True)
	time_stamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
