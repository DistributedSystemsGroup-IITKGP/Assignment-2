# from multiprocessing import Manager

class InMemoryLogQueue:

	def __init__(self):
		# self.manager = Manager()
		self.queue = {}
		self.consumers_front = {}
		self.producers = {}

	def create_topic(self, topic_name):
		self.queue[topic_name] = []

	def list_topics(self):
		return list(self.queue.keys())

	def register_consumer(self, consumer_id, consumer_front = 0):
		self.consumers_front[consumer_id] = consumer_front
	
	def register_producer(self, producer_id, topic_name):
		self.producers[producer_id] = topic_name
		
	def enqueue(self, topic_name, log_message):
		self.queue[topic_name].append(log_message)
		return 1

	def dequeue(self, topic_name, consumer_id):
		log_message = self.queue[topic_name][self.consumers_front[consumer_id]]
		self.consumers_front[consumer_id] += 1
		return log_message

	def empty(self, topic_name, consumer_id):
		return self.consumers_front[consumer_id] == len(self.queue[topic_name])

	def size(self, topic_name, consumer_id):
		return len(self.queue[topic_name]) - self.consumers_front[consumer_id]
