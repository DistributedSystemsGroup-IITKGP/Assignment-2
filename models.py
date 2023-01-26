from main import db

# available user roles: Admin, Mentor, Mentee
class Topic(UserMixin, db.Model):
    __tablename__ = "Topic"

    topic_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    topic_name = db.Column(db.String(255), unique=True, nullable=False)
    producer_count = db.Column(db.Integer, default = 0 ,nullable=False)
    consumer_count = db.Column(db.Integer, default = 0 ,nullable=False)
    msg_count = db.Column(db.Integer, default = 0 ,nullable=False)
    producer = db.relationship("Producer")
    consumer = db.relationship("Consumer")
    log = db.relationship("Log")


class Consumer(UserMixin, db.Model):
    __tablename__ = "Consumer"

    consumer_id = db.Column(db.String(30), primary_key=True)
	topic_id = db.Column(db.Integer, db.ForeignKey("Topic"))
    front = db.Column(db.Integer, default=0, nullable=False)

class Producer(UserMixin, db.Model):
    __tablename__ = "Producer"

    producer_id = db.Column(db.String(30), primary_key=True)
	topic_id = db.Column(db.Integer, db.ForeignKey("Topic"))
	log = db.relationship("Log")

class Log(UserMixin, db.Model):
    __tablename__ = "Log"

    log_id = db.Column(db.String(30), primary_key=True)
	topic_id = db.Column(db.Integer, db.ForeignKey("Topic"))
	producer_id = db.Column(db.Integer, db.ForeignKey("Producer"))
    log_msg = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.String(255), nullable=False)
