import ipaddress
import multiprocessing
import threading
import time
import requests

ip_address = "127.0.0.1"

def scanBroker(port):
    try:
        response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.1)
        print(port, response.status_code)
        if response.status_code == 200 and response.json()["message"] == "broker running":
           return port
    except:
       return None

def scanBroker_network():
    pool = multiprocessing.Pool(processes=50)
    ports = [5000+i for i in range(10)]
    results = pool.map(scanBroker, ports)
    active_ports = []
    for ip in results:
        if ip is not None:
            active_ports.append(ip)
            
    return active_ports

def scanBrokerManager(port):
    try:
        response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.5)
        print(port, response.status_code)
        if response.status_code == 200 and response.json()["message"] == "Broker Manager Copy running":
           return port
    except:
        return None

def scanBrokerManager_network():
    pool = multiprocessing.Pool(processes=50)
    ports = [5000+i for i in range(10)]
    results = pool.map(scanBrokerManager, ports)
    active_ports = [ip for ip in results if ip is not None]
    return active_ports

def scanBrokerManagerPrimary(port):
    try:
        response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.5)
        print(port, response.status_code)
        if response.status_code == 200 and response.json()["message"] == "Broker Manager running":
           return port
    except:
        return None

def scanBrokerManagerPrimary_network():
    pool = multiprocessing.Pool(processes=50)
    ports = [5000+i for i in range(10)]
    results = pool.map(scanBrokerManager, ports)
    active_ports = [ip for ip in results if ip is not None]
    return active_ports

def doSearchJob(brokerOrManager = 0):
    # 0 for broker, 1 for manager, 2 for primary broker Manager
    if brokerOrManager == 0:
        ports = scanBroker_network()
    elif brokerOrManager == 1:
        ports = scanBrokerManager_network()
    else:
        ports = scanBrokerManagerPrimary_network()
    return ip_address, ports
    
def run_thread():
    while True:
        doSearchJob()
        time.sleep(10)

if __name__ == '__main__':
    run_thread()