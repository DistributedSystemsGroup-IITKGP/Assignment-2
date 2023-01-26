import questionary

from flask import Flask
# from flask_sqlalchemy import SQLAlchemy

def create_app():
	app = Flask(__name__)
	answer = questionary.select(
	"Which Server do you want to start?",
	choices=["In memory Server", "Server with persistence"]).ask()
	if(answer=="In memory Server"):
		from server import server as server_blueprint
		app.register_blueprint(server_blueprint)
	if(answer=="Server with persistence"):
		# db = SQLAlchemy()
		# db.init_app(app) 
		from server_persistent import server_p as server_blueprint
		app.register_blueprint(server_blueprint)
	
	
	

	return app

if __name__ == '__main__':
    create_app().run(host='127.0.0.1', port=5000, debug=True, use_reloader=False)
