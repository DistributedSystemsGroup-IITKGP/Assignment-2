import ipaddress
import multiprocessing
import threading
import time
import requests

port = '9000'
def scan(ip):
    try:
        response = requests.get(f'http://{ip}:{port}/status', timeout=0.01)
        if response.status_code == 200 and response.json()["message"] == "broker running":
           return ip
    except:
       pass


def scan_network():
    ip_range = '10.145.128.0/17'
    pool = multiprocessing.Pool(processes=50)
    ips = [f"{ip}" for ip in ipaddress.IPv4Network(ip_range)]
    results = pool.map(scan, ips)
    active_ips = [ip for ip in results if ip is not None]
    return active_ips

def do():
    ips = scan_network()
    print(ips)

    
def run_thread():
    while True:
        do()
        time.sleep(10)

if __name__ == '__main__':
    run_thread()