from flask import Flask, jsonify, request
from queue import log_queue

app = Flask(__name__)

topics = {}
consumers = {}
producers = {}

@app.route("/")
def index():
    return "<h1>Welcome to the Distributed Server!</h1>"


