
import threading
from queue import Queue

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
        pass

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
        