from importlib.machinery import SourceFileLoader
import threading
import pandas as pd
import time

sdkSourceFile = SourceFileLoader("myqueue","/mnt/c/Users/ranjan kumar/Distributed-Queue/LogQueueSDK/myqueue.py").load_module()

def producerTest(fileName):
    data = pd.read_csv(fileName, delimiter = '\t', headers = None)
    print(data)
    data = data.drop(columns = [2,4])

    producerTopics = list(set(data[5]))
    broker = "http://192.168.203.237:5000"

    producer = sdkSourceFile.Producer(broker, producerTopics)

    for i in range(data.shape[0]):
        time.sleep(1)
        row = data.iloc[i]
        counter = 0
        while not producer.can_send():
            time.sleep(1)
            counter += 1
            if counter==60:
                break
            continue
        if counter==60:
            break
        producer.send(row[5], row[1])
    producer.stop()

def consumerTest():
    broker = "http://192.168.203.237:5000"

    consumer = sdkSourceFile.Consumer(broker)

    while True:
        time.sleep(1)
        counter = 0
        while not consumer.can_connect():
            time.sleep(1)
            counter += 1
            if counter == 60:
                break
            continue
        if counter==60:
            break
        counter = 0
        while len(consumer.list_topics())==0:
            counter += 1
            if counter == 60:
                break
            continue
        topics = consumer.list_topics()
        for topic in topics:
            if topic not in consumer.get_registered_topics():
                consumer.register(topic)
            if not consumer.can_consume(topic):
                break
            print(consumer.consume(topic))
    consumer.stop()

def runProducers():
    fileNames = ["TestFiles/test_asgn1/producer_{}.txt".format(i) for i in range(1,2)]
    threads = []
    for fileName in fileNames:
        tempThread = threading.Thread(target=producerTest, args = (fileName, ))
        threads.append(tempThread)
        threads[len(threads)-1].start()
    for thread in threads:
        thread.join()

def runConsumers(numConsumers):
    threads = []
    for _ in range(numConsumers):
        tempThread = threading.Thread(target=consumerTest, args = ())
        threads.append(tempThread)
        threads[len(threads)-1].start()
    for thread in threads:
        thread.join()

def testSDK(numConsumers):
    tProducer = threading.Thread(target=runProducers, args = ())
    tConsumer = threading.Thread(target=runConsumers, args = (numConsumers, ))
    tProducer.start()
    tConsumer.start()
    tProducer.join()
    tConsumer.join()

if __name__ == "__main__":
    testSDK(2)