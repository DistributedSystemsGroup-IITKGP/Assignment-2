from flask import Flask
import unittest
import os
from db.config import engine, Base
from server_backup import server_backup as backup_server


class TestServer(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.register_blueprint(backup_server)
        self.client = self.app.test_client()

    def tearDown(self):
        os.system('rm -rf tests/test.db')

    def test_create_topic(self):
        # success
        response = self.client.post(
            '/topics', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(
            data['message'], "Topic 'test_topic' created successfully")

        # failure case if same topic_name is used
        response = self.client.post(
            '/topics', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Topic 'test_topic' already exists")

    def test_list_topic(self):
        # success
        response = self.client.get('/topics')
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['topics']), list)

    def test_register_consumer(self):
        # success on existing topic
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        response = self.client.post(
            '/consumer/register', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['consumer_id']), int)

        # failure is if topic_name does not exsit
        response = self.client.post(
            '/consumer/register', json={'topic_name': 'test_topic_random'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Topic 'test_topic_random' does not exist")

    def test_register_producer(self):
        # success if topic exists
        response = self.client.post(
            '/producer/register', json={'topic_name': 'test_topic'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['producer_id']), int)

        # success if topic does not exist
        response = self.client.post(
            '/producer/register', json={'topic_name': 'test_topic_2'})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['producer_id']), int)

    def test_enqueue(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic'})

        # success
        response = self.client.post('/producer/produce', json={
                                    "topic_name": "test_topic", "producer_id": 1, "log_message": "test log message"})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')

        # failure when topic_name doesn't exist
        response = self.client.post('/producer/produce', json={
                                    "topic_name": "test_topic_notExist", "producer_id": 1, "log_message": "test log message"})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Topic 'test_topic_notExist' does not exist")

        # failure when producer doesn't exist
        response = self.client.post('/producer/produce', json={
                                    "topic_name": "test_topic", "producer_id": 10, "log_message": "test log message"})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid producer_id")

        # failure when topic and producer ID doesn't match
        self.client.post('/topics', json={'topic_name': 'test_topic_2'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic_2'})

        response = self.client.post('/producer/produce', json={
                                    "topic_name": "test_topic", "producer_id": 2, "log_message": "test log message"})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid producer_id")

    def test_dequeue(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic'})
        self.client.post('/consumer/register',
                         json={'topic_name': 'test_topic'})
        self.client.post('/producer/produce', json={
                         "topic_name": "test_topic", "producer_id": 1, "log_message": "test log message"})

        # success
        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic', "consumer_id": 1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(type(data['log_message']), str)

        # failure if topic_name does not exist
        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic_notExist', "consumer_id": 1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Topic 'test_topic_notExist' does not exist")

        # failure if consumer ID does not exist
        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic', "consumer_id": 10})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid consumer_id")

        # failure on topic_name and consumer_id do not match
        self.client.post('/topics', json={'topic_name': 'test_topic_2'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic_2'})
        self.client.post('/consumer/register',
                         json={'topic_name': 'test_topic_2'})
        self.client.post('/producer/produce', json={
                         "topic_name": "test_topic", "producer_id": 2, "log_message": "test log message"})

        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic', "consumer_id": 2})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid consumer_id")

    def test_size(self):
        self.client.post('/topics', json={'topic_name': 'test_topic'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic'})
        self.client.post('/consumer/register',
                         json={'topic_name': 'test_topic'})
        self.client.post('/producer/produce', json={
                         "topic_name": "test_topic", "producer_id": 1, "log_message": "test log message 1"})
        self.client.post('/producer/produce', json={
                         "topic_name": "test_topic", "producer_id": 1, "log_message": "test log message 2"})
        print(self.client.get('/consumer/consume',
                              json={'topic_name': 'test_topic', "consumer_id": 1}).get_json())

        # success
        response = self.client.get(
            '/size', json={"topic_name": "test_topic", "consumer_id": 1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['size'], 1)

        # failure if topic_name does not exist
        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic_notExist', "consumer_id": 1})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Topic 'test_topic_notExist' does not exist")

        # failure if consumer ID does not exist
        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic', "consumer_id": 10})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid consumer_id")

        # failure on topic_name and consumer_id do not match
        self.client.post('/topics', json={'topic_name': 'test_topic_2'})
        self.client.post('/producer/register',
                         json={'topic_name': 'test_topic_2'})
        self.client.post('/consumer/register',
                         json={'topic_name': 'test_topic_2'})
        self.client.post('/producer/produce', json={
                         "topic_name": "test_topic", "producer_id": 2, "log_message": "test log message"})

        response = self.client.get(
            '/consumer/consume', json={'topic_name': 'test_topic', "consumer_id": 2})
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failure')
        self.assertEqual(
            data['message'], "Invalid consumer_id")


if __name__ == '__main__':
    unittest.main()
