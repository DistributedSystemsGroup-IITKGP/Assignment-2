# from multiprocessing import Manager

class InMemoryLogQueue:

	def __init__(self):
		# self.manager = Manager()
		self.queue = {}
		# self.consumers_front = {}
		# self.producers = {}

	def create_topic(self, topic_name, partition_id):
		qname = topic_name+'#'+str(partition_id)
		self.queue[qname] = []

	# def list_topics(self):
	# 	return list(self.queue.keys())

	# def register_consumer(self, consumer_id, consumer_front = 0):
	# 	self.consumers_front[consumer_id] = consumer_front
	
	# def register_producer(self, producer_id, topic_name):
	# 	self.producers[producer_id] = topic_name
		
	def enqueue(self, topic_name, partition_id, log_message, timestamp):
		qname = topic_name+'#'+str(partition_id)
		self.queue[qname].append((log_message, timestamp))
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
