
import threading
from queue import Queue

import settings as S

class KafkaProducer:

    def __init__(self, bootstrap_servers) -> None:
        self.socket = None
        self.bootstrap_servers = bootstrap_servers
        self.message_queue = Queue()
        self.new_message_event = threading.Event()

        threading.Thread(target=self.send_messages, daemon=True).start()
        pass

    def send_messages(self):
        while True:
            self.new_message_event.wait()
            self.new_message_event.clear()
    
    def send(self, message):
        self.message_queue.put(message)

    def __del__(self):
        pass


class Message:
    def __init__(self) -> None:
        pass


class MessageSet:
    def __init__(self) -> None:
        pass


if __name__ == "__main__":

    kafka_producer = KafkaProducer(bootstrap_servers=S.BOOTSTRAP_SERVERS)
