curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/topics

# curl -X GET http://localhost:5000/topics

# curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/consumer/register

# curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/producer/register

# curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "producer_id":"P_T0#0", "log_message":"example log message 1"}' http://localhost:5000/producer/produce

# curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "producer_id":"P_T0#0", "log_message":"example log message 2"}' http://localhost:5000/producer/produce

# curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "consumer_id":"C_T0#0"}' http://localhost:5000/consumer/consume

# curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "consumer_id":"C_T0#0"}' http://localhost:5000/size
