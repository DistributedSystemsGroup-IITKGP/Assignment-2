import subprocess
import requests

def get_list_of_brokers():
	port = '9000'
	p = subprocess.run(["arp", "-a"],  check=True, stdout=subprocess.PIPE)
	out =  p.stdout.decode('UTF-8')

	# Split the output into lines and parse each line to extract IP addresses
	ip_addresses = []
	for line in out.splitlines():
		ip_address = line.split("(")[1].split(")")[0]
		try:
			print(ip_address)
			response = requests.get(f'http://{ip_address}:{port}/status', timeout=0.01)
			
			if response.status_code == 200 and response.json()["message"] == "broker running":
				ip_addresses.append(ip_address)
			print('successful')
		except:
			continue

	return ip_addresses