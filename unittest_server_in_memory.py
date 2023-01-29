from flask import Flask
import unittest
import sys 

class TestServer(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        from server_in_memory import server as in_memory_server
        self.app.register_blueprint(in_memory_server)
        self.client = self.app.test_client()

    def tearDown(self):
        try:
            del sys.modules['server_in_memory']
        except KeyError:
            pass

    def test_create_topic(self):
        response = self.client.post('/topics', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Topic 'test_topic' created successfully")

    def test_list_topic(self):
        response = self.client.get('/topics')
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['topics']), list)

    def test_register_consumer(self):
        response = self.client.post('/consumer/register', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['consumer_id']), int)

    def test_register_producer(self):
        response = self.client.post('/producer/register', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['producer_id']), int)

    def test_enqueue(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register', json={'topic_name': 'test_topic'})

        response = self.client.post('/producer/produce', json={"topic_name":"test_topic", "producer_id":1, "log_message":"test log message"})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')

    def test_dequeue(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register', json={'topic_name': 'test_topic'})
        self.client.post('/consumer/register', json={'topic_name': 'test_topic'})
        self.client.post('/producer/produce', json={"topic_name":"test_topic", "producer_id":1, "log_message":"test log message"})

        response = self.client.get('/consumer/consume', json={'topic_name':'test_topic', "consumer_id":1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['log_message']), str)

    def test_size(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register', json={'topic_name': 'test_topic'})
        self.client.post('/consumer/register', json={'topic_name': 'test_topic'})
        self.client.post('/producer/produce', json={"topic_name":"test_topic", "producer_id":1, "log_message":"test log message 1"})
        self.client.post('/producer/produce', json={"topic_name":"test_topic", "producer_id":1, "log_message":"test log message 2"})
        print(self.client.get('/consumer/consume', json={'topic_name':'test_topic', "consumer_id":1}).get_json())
        print(self.client.get('/consumer/consume', json={'topic_name':'test_topic', "consumer_id":1}).get_json())

        response = self.client.get('/size', json={"topic_name":"test_topic", "consumer_id":1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['size'], 1)        

if __name__ == '__main__':
    unittest.main()
