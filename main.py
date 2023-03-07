import questionary
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from BrokerManager import server as broker_manager
from BrokerManagerReadOnly import server as broker_manager_readonly
from part2.broker_in_memory import server as broker_in_memory
from part2.broker_persistent import server as broker_persistent
import sys

def create_app():
	app = Flask(__name__)
	
	answer = questionary.select(
		"Which Server do you want to start?",
		choices=["Broker In Memory", "Broker Persistent", "Broker Manager Primary", "Broker Manager Secondary"]
		).ask()
	
	if answer=="Broker In Memory":
		app.register_blueprint(broker_in_memory)

	if answer=="Broker Persistent":
		app.register_blueprint(broker_persistent)

	if answer=="Broker Manager Primary":
		app.register_blueprint(broker_manager)
	
	if answer=="Broker Manager Secondary":
		app.register_blueprint(broker_manager_readonly)
	
	return app

port = 5000
if len(sys.argv)>1:
	port = int(sys.argv[1])

if __name__ == '__main__':
    create_app().run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
