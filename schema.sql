-- Table to store topics
CREATE TABLE Topic (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Table to store producers
CREATE TABLE Producer (
    id VARCHAR(255) PRIMARY KEY,
    topic_id INTEGER REFERENCES Topic(id) NOT NULL
);

-- Table to store consumers
CREATE TABLE Consumer (
    id VARCHAR(255) PRIMARY KEY,
    topic_id INTEGER REFERENCES Topic(id) NOT NULL
);

-- Table to store log messages
CREATE TABLE Log (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER REFERENCES Topic(id) NOT NULL,
    producer_id VARCHAR(255) REFERENCES Producer(id) NOT NULL,
    message VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
