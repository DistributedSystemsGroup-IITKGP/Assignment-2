from flask import Blueprint, jsonify, request
from queue_mem import log_queue

server_p = Blueprint("server",__name__)