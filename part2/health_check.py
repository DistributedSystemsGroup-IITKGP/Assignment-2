import ipaddress
import multiprocessing
import threading
import time
import requests

ip_address = "127.0.0.1"

def scanBroker(port):
    try:
        response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.01)
        if response.status_code == 200 and response.json()["message"] == "broker running":
           return port, True, response.json()["broker_id"]
        if response.status_code == 200 and response.json()["message"] == "broker replica running":
           return port, False, response.json()["broker_id"]
    except:
       return None

def scanBroker_network():
    pool = multiprocessing.Pool(processes=50)
    ports = [5000+i for i in range(10)]
    results = pool.map(scanBroker, ports)
    active_ports = []
    active_ports_replica = [] 
    for ip in results:
        if ip is not None:
            if ip[1]:
                active_ports.append((ip[0], ip[2]))
            else:
                active_ports_replica.append((ip[0], ip[2]))

    return active_ports, active_ports_replica

def scanBrokerManager(port):
    try:
        response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.01)
        if response.status_code == 200 and response.json()["message"] == "Broker Manager Copy running":
           return port
    except:
       return None

def scanBrokerManager_network():
    pool = multiprocessing.Pool(processes=50)
    ports = [5000+i for i in range(10)]
    results = pool.map(scanBrokerManager, ports)
    active_ports = [ip for ip in results if ip is not None]
    return active_ports, None

def doSearchJob(brokerOrManager: int):
    # 0 for broker, 1 for manager
    if brokerOrManager == 0:
        ports, portsReplica = scanBroker_network()
    else:
        ports, portsReplica = scanBrokerManager_network()
    return ip_address, ports, portsReplica
    
def run_thread():
    while True:
        doSearchJob()
        time.sleep(10)

if __name__ == '__main__':
    run_thread()