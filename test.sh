curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/topics

curl -X GET http://localhost:5000/topics

# curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/consumer/register
curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic1"}' http://localhost:5000/consumer/register

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic"}' http://localhost:5000/producer/register
curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic1"}' http://localhost:5000/producer/register

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "producer_id":1, "log_message":"example log message 1"}' http://localhost:5000/producer/produce

curl -X POST -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "producer_id":2, "log_message":"example log message 2"}' http://localhost:5000/producer/produce

curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "consumer_id":1}' http://localhost:5000/consumer/consume

curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "consumer_id":1}' http://localhost:5000/size
curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic", "consumer_id":2}' http://localhost:5000/size
curl -X GET -H "Content-Type: application/json" -d '{"topic_name":"example_topic1", "consumer_id":2}' http://localhost:5000/size
