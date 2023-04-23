


import os
import json
from glob import glob
import threading
import time
import logging
import shutil
import socket

import settings as S


class ThreadSafeDict:
    def __init__(self):
        self._lock = threading.Lock()
        self._state: dict = {}

    def __iter__(self):
        return self._state.__iter__()

    def __setitem__(self, key, value):
        with self._lock:
            self._state[key] = value

    def __getitem__(self, key):
        with self._lock:
            return self._state[key]

    def __contains__(self, key):
        return (key in self._state)

    def __str__(self) -> str:
        return self._state.__str__()

    def get(self, key, default=None):
        with self._lock:
            return self._state.get(key, default)
    
    def get_dict(self):
        with self._lock:
            return self._state.copy()
    
    def values(self):
        with self._lock:
            return self._state.values()
    
    def keys(self):
        with self._lock:
            return self._state.keys()


class FileManager:
    def __init__(self, root) -> None:
        self.root = root
        self.topic_root = os.path.join(root, "topics")
        os.makedirs(self.topic_root, exist_ok=True)
        # for i in S.SERVER_IDS:
        #     os.makedirs(os.path.join(self.topic_root, str(i)), exist_ok=True)
        self.offsets = ThreadSafeDict()
        self.message_index = ThreadSafeDict()
        self.segment_files = ThreadSafeDict()
        self.topic_locks = ThreadSafeDict()
        self.create_topic_lock = threading.Lock()
        self.delete_thread = threading.Thread(target=self.delete_old_files_thread, daemon=True)
        self.delete_thread.start()

        
    def delete_old_files_thread(self):
        while True:
            time.sleep(S.DELETE_FILES_INTERVAL)
            try:
                current_time = time.time()

                for root, dirs, files in os.walk(self.root, topdown=True):
                    for name in files:
                        fpath = os.path.join(root, name)
                        fmodified = os.path.getmtime(fpath)
                        if current_time - fmodified > S.TOPIC_RETENTION_SLA:
                            try:
                                os.remove(fpath)
                                logging.info(f"DELETED FILE {fmodified} {fpath}")
                            except:
                                pass
                    # for name in dirs:
                    #     fpath = os.path.join(root, name)
                    #     fmodified = os.path.getmtime(fpath)
                    #     if current_time - fmodified > S.TOPIC_RETENTION_SLA:
                    #         try:
                    #             shutil.rmtree(fpath)
                    #             logging.info(f"DELETED DIR {fmodified} {fpath}")
                    #         except:
                    #             pass
            except:
                pass
    

    def get_topic_lock(self, topic):
        if self.topic_locks.get(topic) is None:
            with self.create_topic_lock:
                if self.topic_locks.get(topic) is None:
                    self.topic_locks[topic] = threading.Lock()
        return self.topic_locks.get(topic)
    
    def send_file(self, conn: socket.socket, topic, offset):
        path = os.path.join(self.topic_root, topic)
        with open(path, 'rb') as rf:
            sent_count = conn.sendfile(rf, offset=offset, count=1)


    def write_to_topic(self, topic, bytes):
        try:
            with self.get_topic_lock(topic):
                path = os.path.join(self.topic_root, topic)
                file_desc = os.open(path, os.O_RDWR | os.O_CREAT)
                os.pwrite(file_desc, bytes, self.offsets.get(topic, 0))     
                os.close(file_desc)
                if topic not in self.offsets:
                    self.offsets[topic] = 0
                previous_offset = self.offsets[topic]
                self.offsets[topic] += len(bytes)
                return previous_offset
        except Exception as e:
            print(f'Error in writing to topic {topic}: {e}')

    
    # def write(self, file, message):
    #     if isinstance(message, dict):
    #         message = json.dumps(message)
    #     path = os.path.join(self.root, file)
    #     with open(path, 'w') as f:
    #         f.write(message)
    
    # def fast_write(self, file, message: bytes):
    #     try:
    #         path = os.path.join(self.fast_root, file)
    #         file_desc = os.open(path, os.O_RDWR | os.O_CREAT)
    #         os.write(file_desc, message)     
    #         os.close(file_desc)
    #     except Exception as e:
    #         print(f'Error in fast_write: {e}')

    # def read(self, file):
    #     path = os.path.join(self.root, file)
    #     if glob(path):
    #         with open(path, 'r') as f:
    #             lines = f.read()
    #         return lines
    
    # def fast_read(self, file) -> bytes:
    #     try:
    #         path = os.path.join(self.fast_root, file)
    #         file_size = os.path.getsize(path)
    #         file_desc = os.open(path, os.O_RDONLY)
    #         lines = os.read(file_desc, file_size)
    #         os.close(file_desc)
    #         return lines
    #     except Exception as e:
    #         print(f'Error in fast_read: {e}')

    # def readlines(self, file):
    #     path = os.path.join(self.root, file)
    #     if glob(path):
    #         with open(path, 'r') as f:
    #             lines = f.readlines()
    #         return lines
    
    # def append(self, file, message):
    #     if isinstance(message, dict):
    #         message = json.dumps(message)
    #     path = os.path.join(self.root, file)
    #     with open(path, 'a') as f:
    #         f.write(message)
    #         f.write('\n')
    
    # def delete_file(self, file, fast=False):
    #     try:
    #         if fast:
    #             os.remove(os.path.join(self.fast_root, file))
    #         else:
    #             os.remove(os.path.join(self.root, file))
    #     except OSError:
    #         pass
    
    # def list_files(self, path=None, fast=False):
    #     if fast:
    #         if path:
    #             return os.listdir(os.path.join(self.fast_root, path))
    #         return os.listdir(self.fast_root)
    #     if path:
    #         return os.listdir(os.path.join(self.root, path))
    #     return os.listdir(self.root)
