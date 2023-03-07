# from multiprocessing import Manager

class InMemoryLogQueue:

	def __init__(self):
		self.queue = {}

	def create_topic(self, topic_name, partition_id):
		qname = topic_name+'#'+str(partition_id)
		self.queue[qname] = []
		
	def enqueue(self, topic_name, partition_id, producer_id, log_message, timestamp):
		qname = topic_name+'#'+str(partition_id)
		self.queue[qname].append((log_message, producer_id, timestamp))
		return 1

	def dequeue(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		return self.queue[qname][consumer_front]

	def empty(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		return consumer_front == len(self.queue[qname])

	def size(self, topic_name, partition_id, consumer_front):
		qname = topic_name+'#'+str(partition_id)
		return len(self.queue[qname]) - consumer_front
