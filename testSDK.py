from importlib.machinery import SourceFileLoader
import os
import threading
import pandas as pd
import time

path = os.path.abspath('..') + '/Distributed-Queue/LogQueueSDK/myqueue.py'
sdkSourceFile = SourceFileLoader("myqueue",path).load_module()

def producerTest(fileName):
    data = pd.read_csv(fileName, delimiter = '\t', header = None)
    data = data.drop(columns = [2,4])

    producerTopics = list(set(data[5]))
    broker = "localhost:5000"

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

def consumerTest(topics):
    broker = "localhost:5000"
    consumer = sdkSourceFile.Consumer(broker, topics)

    while True:
        time.sleep(1)
        counter = 0

        while True:
            res = consumer.get_next()
            if res is None:
                break
            print("Consumer Message - {}".format(res))
    consumer.stop()

def runProducers():
    fileNames = ["TestFiles/test_asgn1/producer_{}.txt".format(i) for i in range(1,6)]
    threads = []


    topics = ['T-1', 'T-2', 'T-3']

    for fileName in fileNames:
        tempThread = threading.Thread(target=producerTest, args = (fileName, ))
        threads.append(tempThread)
        threads[len(threads)-1].start()
    for thread in threads:
        thread.join()

def runConsumers():
    threads = []
    topics = [['T-1', 'T-2', 'T-3'], ['T-1', 'T-3'], ['T-1', 'T-3']]
    for i in range(3):
        tempThread = threading.Thread(target=consumerTest, args = (topics[i], ))
        threads.append(tempThread)
        threads[len(threads)-1].start()
    for thread in threads:
        thread.join()

def testSDK():
    tProducer = threading.Thread(target=runProducers, args = ())
    tProducer.start()
    time.sleep(10)
    tConsumer = threading.Thread(target=runConsumers, args = ())
    tConsumer.start()
    tProducer.join()
    tConsumer.join()

if __name__ == "__main__":
    testSDK()