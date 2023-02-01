from myqueue import Producer, Consumer
from time import sleep


def produce():
    producer = Producer(
        topics=['topic1', 'topic2'],
        broker='localhost:5000'
    )
    
    try:
        for i in range(20):
            producer.send('topic1', 'DEBUG')
            producer.send('topic2', 'INFO')

    finally:
        producer.stop()


from myqueue import Consumer

def consume():
    consumer = Consumer(
        topics=[ 'topic1', 'topic2'],
        broker='localhost:5000'
    )
    try:
        while True:
            res = consumer.get_next()
            if res is None:
                break
            print("Consumer Message - {}".format(res))

    finally:
        consumer.stop()


produce()
consume()