
import logging
import socket
import threading
from queue import Queue
from kazoo.client import KazooClient

import settings as S
import constants as C

class KafkaConsumer:

    def __init__(self, bootstrap_servers, topic) -> None:
        self.events = {}
        self.event_listener = threading.Event()
        self.socket = None
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.offsets_queue = Queue()
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
                # print("Data is %s" % data)
                # print("Version is %s" % stat.version)
                # print(event)
                self.offsets_queue.put(data)
                self.event_listener.set()
        except Exception as e:
            logging.error(e)



    def createMessageStreams(self):
        READ_COMMAND = f'r{self.topic}'.encode()
        self.socket = None
        while True:
            try:
                if self.socket is None:
                    HOST, PORT = self.bootstrap_servers.split(':')
                    PORT = int(PORT)
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.connect((HOST, PORT))
                
                while self.offsets_queue.qsize():
                    offset = self.offsets_queue.queue[0]
                    if offset:
                        message =  READ_COMMAND + offset
                        # print(message)
                        self.socket.sendall(message)

                        r = self.socket.recv(S.BYTES_PER_MESSAGE)
                        yield r
                    self.offsets_queue.get(0)
            except socket.error:
                self.socket.close()
                self.socket = None
            except Exception as e:
                logging.error(e)
                # raise e
            self.event_listener.wait()
            self.event_listener.clear()



if __name__ == "__main__":


    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)

    topic = 'TOPIC'

    kafka_consumer = KafkaConsumer(bootstrap_servers=S.BOOTSTRAP_SERVERS, topic=topic)

    for message in kafka_consumer.createMessageStreams():
        if len(message) > 0 and message != b' ':
            print(message.decode())
