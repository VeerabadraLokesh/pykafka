


## Run zookeeper
docker run --rm --net=host --name kafka-zookeeper -d zookeeper

## kill zookeeper
docker rm -f kafka-zookeeper

## Run Broker
python KafkaBroker.py


## Run Kafka Consumer
python KafkaConsumer.py

## Run Kafka Producer
python KafkaProducer.py


## Run Function producer
python test_function_producer.py

## Run Function consumer
python test_function_consumer.py


## Run Peformance Test

### Run test data producer
python test_producer.py

### Run test data consumer
python test_consumer.py

