import questionary
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# db = SQLAlchemy()


def create_app():
	app = Flask(__name__)
	
	answer = questionary.select(
		"Which Server do you want to start?",
		choices=["In memory Server", "Server with persistence"]
		).ask()
	
	if answer=="In memory Server":
		app.register_blueprint(in_memory_server)

	if answer=="Server with persistence":
		# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite' 
		app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False 
		# db.init_app(app) 
		app.register_blueprint(persistent_server)
	
	return app


if __name__ == '__main__':
    create_app().run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)
