
import threading
from queue import Queue
from kazoo.client import KazooClient
import socket

from file_manager import FileManager
import settings as S



class KafkaBroker:

    def __init__(self, file_manager: FileManager) -> None:
        self.flush_message_event = threading.Event()
        self.flush_message_lock = threading.Lock()
        self.flush_message_counter = 0
        self.messages = {}
        self.file_manager = file_manager
        self.message_log = {}
        threading.Thread(target=self.connect_to_zookeeper, daemon=True).start()
        pass

    def connect_to_zookeeper(self):
        self.zk = KazooClient(hosts=S.ZOOKEEPER_SERVICE)
        self.zk.start()

    def handle_client_requests(self, conn):
        while True:
            data = conn.recv(S.BYTES_PER_MESSAGE)
            if not data:
                break
            command = data[0]
            topic = data[1:6]
            payload = data[6:]
            print(command, topic, len(payload))
        pass
    
    def listen(self):
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((S.BROKER_SOCKET_HOST, S.CLIENTS_PORT))
            s.listen(S.BROKER_CONNECTIONS)
            print(f"Listening on {S.BROKER_SOCKET_HOST}:{S.CLIENTS_PORT}")
            while True:
                try:
                    conn, addr = s.accept()
                    print('Connected to :', addr[0], ':', addr[1])
                    threading.Thread(target=self.handle_client_requests, args=[conn], daemon=True).start()
                except:
                    pass
        finally:
            if s is not None:
                s.close()

    def write_to_disk(self):

        while True:

            with self.flush_message_lock:
                self.flush_message_counter = 0
            
            for topic in self.messages:
                topic_messages_queue = self.messages[topic]
                while topic_messages_queue.qsize():
                    payload = topic_messages_queue.queue[0]

                    try:
                        self.file_manager.write_to_topic(topic, payload)
                        if topic not in self.message_log:
                            self.message_log[topic] = [0]
                        ## expose message to client only after message is flushed to disk
                        self.message_log.append(self.message_log[-1]+len(payload))
                        topic_messages_queue.get(0)
                    except Exception as e:
                        pass

            self.flush_message_event.wait(timeout=S.FLUSH_MESSAGE_TIMEOUT)
    
    def write_to_topic(self, topic, message):
        bytes = message.payload
        

        with self.flush_message_lock:
            self.flush_message_counter += 1
            if self.flush_message_counter > S.FLUSH_MESSAGE_COUNT:
                self.flush_message_event.set()

if __name__ == "__main__":
    file_manager = FileManager(S.KAFKA_STORAGE_PATH)

    kafka_broker = KafkaBroker(file_manager=file_manager)
