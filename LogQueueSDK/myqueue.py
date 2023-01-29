import requests


####################################################################################
#
# @brief The Base Class implementing all the common functionalities of a Client
#
# This class contains the methods to create topics and list all the topics.
# They are included here as they are methods shared by both producers and consumers.
#
# Attributes:
#     * broker
#         the address to the server broker
#     * topics
#         the list of topics that the client has registered to
#
# Methods:
#    
#     @tparam topic
#     * create_topic
#         requests the server to create the topic given by the param topic
#         and add it to the distributed queue. Returns boolean.
#    
#     @tparam None
#     * list_topics
#         requests the list of topics available on the server. Returns a list
#         of topics received from the server.
#    
####################################################################################
class Client:
    
    def __init__(self, broker, topics = None):
        self.broker = broker
        self.topics = topics
    
    def create_topic(self, topic : str) -> bool:
        params = {
            "topic_name" : topic
        }
        r = requests.post(url = self.broker + '/topics', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not create topic \"', topic, '\"!')
            print('Error Message : ', response["message"])
            return False
        else:
            print(response["message"])
            return True
    
    def list_topics(self) -> list:
        r = requests.get(url = self.broker + '/topics', params = None)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not receive list of topics!')
            print('Error Message : ', response["message"])
            return []
        else:
            print('List of Topics are - \n')
            for i, topic in enumerate(response["topics"]):
                print('Topic ', i, ' : ', topic)
            return response["topics"]




####################################################################################
#
# @brief The Class implementing a Producer. Inherited from Client
#
# This class contains the methods to register as a producer with the queue and
# enqueue log messages into the queue.
#
# Attributes:
#     * broker
#         the address to the server broker
#     * topics
#         the list of topics that the client has registered to
#     * topics_ids_map
#         the map of all the registered topics by the client and their correspondingly
#         allocated IDs.
#
# Methods:
#    
#     @tparam topic
#     * register
#         requests the server to register the producer to the given topic in the queue
#         returns boolean.
#    
#     @tparam topic
#     @tparam producer_id
#     @tparam message
#     * enqueue
#         adds the log message given by message to the queue corresponding to the topic
#         and is added using the identity producer_id
#    
####################################################################################
class Producer(Client):
    
    def __init__(self, broker, topics = None):
        self.topics = topics
        self.broker = broker
        self.topic_id_map = {}
        for topic in topics:
            response = self.register(topic)
            if response == -1:
                print('Trying to create the topic on the queue!')
                
                if self.create_topic(topic):
                    response = self.register(topic)
                    if response == -1:
                        print('Unable to register to the topic even after creating it!')
                else:
                    print('Unable to create the topic on the queue!')   
            else:
                print('Registered to the topic ', topic, ' as a Producer!')
    
    def register(self, topic : str) -> int:
        params = {
            "topic_name" : topic
        }
        r = requests.post(url = self.broker + '/producer/register', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not register to the topic - \"', topic, "\" as a Producer!")
            print('Error Message : ', response["message"])
            return -1
        else:
            self.topic_id_map[topic] = response["producer_id"]
            return response["producer_id"]
    
    def enqueue(self, topic : str, producer_id : int, message : str) -> bool:
        params = {
            "topic_name" : topic,
            "producer_id" : producer_id,
            "message" : message
        }
        r = requests.post(url = self.broker + '/producer/produce', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not enqueue into the topic - \"', topic, "\"!")
            print('Error Message : ', response["message"])
            return False
        else:
            return True




####################################################################################
#
# @brief The Class implementing a Consumer. Inherited from Client
#
# This class contains the methods to register as a consumer with the queue and
# dequeue log messages from the queue.
#
# Attributes:
#     * broker
#         the address to the server broker
#     * topics
#         the list of topics that the client has registered to
#     * topics_ids_map
#         the map of all the registered topics by the client and their correspondingly
#         allocated IDs.
#
# Methods:
#    
#     @tparam topic
#     * register
#         requests the server to register the consumer to the given topic in the queue
#         returns boolean.
#    
#     @tparam topic
#     @tparam consumer_id
#     * dequeue
#         removes the last log message in the queue corresponding to the topic and returns
#         it the specified consumer.
#
#     @tparam topic
#     @tparam consumer_id
#     * size
#         returns the size of the queue given by the topic.
#    
####################################################################################
class Consumer(Client):
    
    def __init__(self, broker, topics = None):
        self.topics = topics
        self.broker = broker
        self.topic_id_map = {}
        for topic in topics:
            response = self.register(topic)
    
    
    def register(self, topic : str) -> int:
        params = {
            "topic_name" : topic
        }
        r = requests.post(url = self.broker + '/consumer/register', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not register to the topic - \"', topic, "\" as a Consumer!")
            print('Error Message : ', response["message"])
            return -1
        else:
            self.topic_id_map[topic] = response["consumer_id"]
            return response["consumer_id"]
    
    
    def dequeue(self, topic : str, consumer_id : int) -> str:
        params = {
            "topic_name" : topic,
            "consumer_id" : consumer_id
        }
        r = requests.get(url = self.broker + '/consumer/consume', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not dequeue from the topic - \"', topic, "\"!")
        return response["message"]
    
    
    def size(self, topic : str, consumer_id : int) -> int:
        params = {
            "topic_name" : topic,
            "size" : consumer_id
        }
        r = requests.get(url = self.broker + '/size', params = params)
        response = r.json()
        if response["status"] != "success":
            print('ERROR : Could not retrieve the number of log messages in the requested topic - \"', topic, "\"!")
            print('Error Message : ', response["message"])
            return -1
        else:
            return response["size"]