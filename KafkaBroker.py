
import threading
from queue import Queue
from kazoo.client import KazooClient
import socket
import logging
from file_manager import FileManager, ThreadSafeDict
import time

import constants as C
import settings as S



class KafkaBroker:

    def __init__(self, file_manager: FileManager) -> None:
        self.flush_message_event = threading.Event()
        self.flush_message_lock = threading.Lock()
        self.flush_message_counter = 0
        self.messages_queue = Queue()
        self.file_manager = file_manager
        self.message_log = {}
        self.topics = ThreadSafeDict()
        threading.Thread(target=self.connect_to_zookeeper, daemon=True).start()
        threading.Thread(target=self.write_to_disk, daemon=True).start()
        # self.previous_offset = -1
        pass

    def connect_to_zookeeper(self):
        try:
            self.zk = KazooClient(hosts=S.ZOOKEEPER_SERVICE)
            self.zk.start()
        except Exception as e:
            logging.error(e)

    def update_zookeeper(self, topic, offset):
        topic_path = f"/topics/{topic}"
        if self.topics.get(topic) is None:
            self.zk.ensure_path(topic_path)
            self.topics[topic] = 1
        # current_offset = int(offset)/S.TEST_MESSAGE_SIZE
        # if current_offset - self.previous_offset > 1:
        #     logging.error(f"{current_offset} | {self.previous_offset}")
        # else:
        #     logging.info(current_offset)
        # self.previous_offset = current_offset
        self.zk.set(topic_path, str(offset).encode())

    def handle_client_requests(self, conn):
        while True:
            data = conn.recv(S.BYTES_PER_MESSAGE)
            if not data:
                break
            try:
                command = data[0]
                topic = data[1:6].decode()
                payload = data[6:]
                if command == 119 or command == 'w':
                    # print(command, topic, len(payload))
                    self.write_to_topic(topic, payload)
                    conn.sendall(C.SUCCESS)
                elif command == 114 or command == 'r':
                    offset = int(payload.decode())
                    # print(command, topic, offset)
                    ## Send data efficiently using sendfile api
                    file_manager.send_file(conn, topic, offset)
                    # print('sent successfully')
            except:
                conn.sendall(C.FAILED)
    
    def listen(self):
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((S.BROKER_SOCKET_HOST, S.CLIENTS_PORT))
            s.listen(S.BROKER_CONNECTIONS)
            print(f"Listening on {S.BROKER_SOCKET_HOST}:{S.CLIENTS_PORT}")
            while True:
                try:
                    conn, addr = s.accept()
                    print('Connected to :', addr[0], ':', addr[1])
                    threading.Thread(target=self.handle_client_requests, args=[conn], daemon=True).start()
                except KeyboardInterrupt:
                    logging.info("Received KeyboardInterrupt")
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                    break
                except Exception as e:
                    logging.error(e)
                    pass
        finally:
            if s is not None:
                s.close()

    def write_to_disk(self):

        while True:

            with self.flush_message_lock:
                self.flush_message_counter = 0
            
            # count = 0
            # start_time = time.perf_counter()
            
            while self.messages_queue.qsize():
                try:
                    topic, payload = self.messages_queue.queue[0]
                    offset = self.file_manager.write_to_topic(topic, payload)
                    self.update_zookeeper(topic, offset)
                    self.messages_queue.get(0)
                    # count += 1
                except Exception as e:
                    raise e
            # end_time = time.perf_counter()
            # time_diff = end_time - start_time
            # time_per_msg = count/time_diff
            # if count > 0:
            #     logging.info(f"saved {count} messags in {time_diff}: {time_per_msg} msgs rate")

            # for topic in self.messages:
            #     topic_messages_queue = self.messages[topic]
            #     while topic_messages_queue.qsize():
            #         payload = topic_messages_queue.queue[0]

            #         try:
            #             self.file_manager.write_to_topic(topic, payload)
            #             if topic not in self.message_log:
            #                 self.message_log[topic] = [0]
            #             ## expose message to client only after message is flushed to disk
            #             self.message_log.append(self.message_log[-1]+len(payload))
            #             topic_messages_queue.get(0)
            #         except Exception as e:
            #             pass

            self.flush_message_event.wait(timeout=S.FLUSH_MESSAGE_TIMEOUT)
            self.flush_message_event.clear()
    
    def write_to_topic(self, topic, payload):
        
        qobj = (topic, payload)
        self.messages_queue.put(qobj)

        if self.messages_queue.qsize() > S.FLUSH_MESSAGE_COUNT and not self.flush_message_event.is_set():
            self.flush_message_event.set()

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)

    file_manager = FileManager(S.KAFKA_STORAGE_PATH)

    kafka_broker = KafkaBroker(file_manager=file_manager)

    kafka_broker.listen()
