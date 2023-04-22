

#docker run --name some-zookeeper --restart always -d zookeeper


# docker run --name kafka-zookeeper -d zookeeper --net=host
docker run --rm --net=host --name kafka-zookeeper -d zookeeper

