
import threading
from queue import Queue
import socket
import logging
from time import sleep

import constants as C
import settings as S

class KafkaProducer:

    def __init__(self, bootstrap_servers) -> None:
        self.socket = None
        self.bootstrap_servers = bootstrap_servers
        # self.topic = topic
        self.message_queue = Queue()
        self.new_message_event = threading.Event()
        self.wait_event = threading.Event()

        threading.Thread(target=self.send_messages, daemon=True).start()
        pass

    ## Wait till all messages are sent
    def wait(self):
        # previous_count = self.message_queue.qsize()
        while self.message_queue.qsize():
            current_count = self.message_queue.qsize()
            # if current_count - previous_count > 1000:
            #     previous_count = current_count
            logging.info(f"{self.message_queue.qsize()} messages remaining")
            self.wait_event(1)
            if self.wait_event.is_set():
                self.wait_event.clear()

    def send_messages(self):
        self.socket = None
        while True:
            try:
                if self.socket is None:
                    HOST, PORT = self.bootstrap_servers.split(':')
                    PORT = int(PORT)
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.connect((HOST, PORT))
                
                while self.message_queue.qsize():
                    message = self.message_queue.queue[0]
                    self.socket.sendall(message)

                    r = self.socket.recv(S.BYTES_PER_MESSAGE)
                    if r == C.SUCCESS:
                        self.message_queue.get(0)
                    else:
                        pass
                self.wait_event.set()
            except socket.error:
                # print(self.message_queue.qsize())
                self.socket.close()
                self.socket = None
            except Exception as e:
                logging.error(e)
            self.new_message_event.wait(timeout=S.FLUSH_MESSAGE_TIMEOUT)
            if self.new_message_event.is_set():
                self.new_message_event.clear()
    
    def send(self, topic, message):
        if len(message) < 1:
            return
        message_ = bytes(f'w{topic}', 'utf-8') + message
        self.message_queue.put(message_)
        self.new_message_event.set()

    def __del__(self):
        try:
            if self.socket is not None:
                self.socket.close()
        except:
            pass
        pass


class Message:
    def __init__(self) -> None:
        pass


class MessageSet:
    def __init__(self) -> None:
        pass


if __name__ == "__main__":


    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)

    topic = "TOPIC"

    kafka_producer = KafkaProducer(bootstrap_servers=S.BOOTSTRAP_SERVERS)

    while True:
        i = input("enter message: ")
        if i == 'q':
            del kafka_producer
            break
        m = bytes(i, 'utf-8')
        kafka_producer.send(topic, m)
    # kafka_producer.wait()
