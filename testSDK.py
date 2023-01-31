from importlib.machinery import SourceFileLoader
import threading
import pandas as pd
import time

sdkSourceFile = SourceFileLoader("myqueue","/home/ec2-user/Distributed-Queue/LogQueueSDK/myqueue.py").load_module()

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
        print("Success")
    producer.stop()

def consumerTest():
    broker = "localhost:5000"
    topics = ['T-1', 'T-2', 'T-3']
    consumer = sdkSourceFile.Consumer(broker, topics)

    while True:
        time.sleep(1)
        counter = 0
        
        while True:
            res = consumer.get_next()
            print(res)
            if res is None:
                break
            print(res)
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

def runConsumers(numConsumers):
    threads = []
    for _ in range(numConsumers):
        tempThread = threading.Thread(target=consumerTest, args = ())
        threads.append(tempThread)
        threads[len(threads)-1].start()
    for thread in threads:
        thread.join()

def testSDK(numConsumers):
    # tProducer = threading.Thread(target=runProducers, args = ())
    # tProducer.start()
    # time.sleep(10)
    tConsumer = threading.Thread(target=runConsumers, args = (numConsumers, ))
    tConsumer.start()
    # tProducer.join()
    tConsumer.join()

if __name__ == "__main__":
    testSDK(2)