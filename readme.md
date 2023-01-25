<h2>Distributed Queue</h2>

<h3>Group members:<h3>

<li>Abhilash Datta</li>
<li>Sunanda Mandal</li>
<li>Rohit Raj</li>
<li>Haasita Pinnepu</li>
<li>Matta Varun</li>
<li>Bhaskar</li>

<h3>System Specification<h3>
<li>OS: Ubuntu 22.04.1 LTS<li> 
<li>Python: 3.10.6<li> 
<li>Libraries: refer to [requirements.txt](https://github.com/DistributedSystemsGroup-IITKGP/Distributed-Queue/blob/main/requirements.txt "requirements") for details<li> 

<h3>How to run:<h3>
	1. Clone the repository
		`git clone https://github.com/DistributedSystemsGroup-IITKGP/Distributed-Queue.git`
	2. Setup Virtual Environment
       `python -m venv .venv`
	3. Activate virtual env
		`source .venv/bin/activate`
	4. Run Server
		`flask --app server --debug run`
	5. Test
		`./test.sh`
