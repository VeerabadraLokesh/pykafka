
import logging
import socket
import threading
from queue import Queue
from kazoo.client import KazooClient

import settings as S

class KafkaConsumer:

    def __init__(self, bootstrap_servers, topic) -> None:
        self.events = {}
        self.event_listener = threading.Event()
        self.socket = None
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        # self.topic = topic
        # self.message_queue = Queue()
        threading.Thread(target=self.connect_to_zookeeper, daemon=True).start()
        pass

    def connect_to_zookeeper(self):
        try:
            self.zk = KazooClient(hosts=S.ZOOKEEPER_SERVICE)
            self.zk.start()

            @self.zk.DataWatch(path=f'/topics/{self.topic}')
            def my_func(data, stat, event):
                print("Data is %s" % data)
                print("Version is %s" % stat.version)
                print(event)
        except Exception as e:
            logging.error(e)



    def createMessageStreams(self):
        while True:
            # messages = {}
            # for message in messages:
            #     bytes = message.payload
            #     yield bytes
            # self.events[topic].wait()
            # self.events[topic].clear()
            self.event_listener.wait()
            self.event_listener.clear()
        pass


if __name__ == "__main__":

    topic = 'TOPIC'

    kafka_consumer = KafkaConsumer(bootstrap_servers=S.BOOTSTRAP_SERVERS, topic=topic)

    kafka_consumer.createMessageStreams()
