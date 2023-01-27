from flask import Blueprint, jsonify, request
from log_queue import PersistentLogQueue

server_p = Blueprint("server",__name__)