# PyKafka - A python implementation of Apache Kafka

This is a python implementation of Kafka using ZooKeeper.

## System setup and basic message delivery

### Build docker image

`docker build -t pykafka .`

### Create network bridge
`docker network create --driver bridge cs2510`

### Run ZooKeeper
`docker run --rm --network=cs2510 --name kafka-zookeeper -d zookeeper`

### Run docker image
`docker run --rm -it --network=cs2510 --name pykafka pykafka:latest bash`

### Run Broker in the docker
`python KafkaBroker.py`

### Login into the same docker container from two terminals to run Producer and Consumer
`docker exec -it pykafka bash`

### Run Consumer
`python KafkaConsumer.py`

### Run Producer
`python KafkaProducer.py`

Send messages in producer and they will appear on consumer. 'q' will exit the producer.

## Test Function Producer and Consumer

While logged into docker container, run below commands to run function consumer and producers

### Function Consumer
`python test_function_consumer.py`

### Function Producer
`python test_function_producer.py --number 16`

## Performance Test

### Run Producer
`python test_producer.py`

### Run Consumer
`python test_consumer.py`

## Video demonstration

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/ebLco5ngHhI/0.jpg)](https://youtu.be/ebLco5ngHhI)
