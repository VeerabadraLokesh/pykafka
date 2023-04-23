


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
    
    def __delitem__(self, key):
        with self._lock:
            if key in self._state:
                del self._state[key]

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
        ## deleting old messages
        if os.path.isdir(self.topic_root):
            shutil.rmtree(self.topic_root)
        os.makedirs(self.topic_root, exist_ok=True)
        # for i in S.SERVER_IDS:
        #     os.makedirs(os.path.join(self.topic_root, str(i)), exist_ok=True)
        self.offsets = ThreadSafeDict()
        # self.message_index = ThreadSafeDict()
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

                for topic in self.segment_files:
                    with self.get_topic_lock(topic):
                        active_topic_file = self.segment_files[topic]['active']
                        path = os.path.join(self.topic_root, active_topic_file)
                        active_file_size = os.path.getsize(path)
                        # logging.info(f"{active_topic_file} size {active_file_size}, {active_file_size > S.MAX_SEGMENT_FILE_SIZE}")
                        if active_file_size > S.MAX_SEGMENT_FILE_SIZE:
                            # offset = int(active_topic_file.split('_')[1])
                            current_offset = self.offsets[topic]['offset']
                            # logging.info(current_offset)
                            new_topic_file = f'{topic}_{current_offset}'
                            # logging.info(new_topic_file)
                            self.segment_files[topic]['active'] = new_topic_file
                            self.segment_files[topic][current_offset] = new_topic_file
                            offsets = list(self.segment_files[topic].keys())
                            offsets.remove('active')
                            offsets.sort(reverse=True)
                            self.segment_files[topic]['offsets'] = offsets

                for root, dirs, files in os.walk(self.root, topdown=True):
                    for name in files:
                        fpath = os.path.join(root, name)
                        fmodified = os.path.getmtime(fpath)
                        if current_time - fmodified > S.TOPIC_RETENTION_SLA:
                            try:
                                if self.segment_files[topic]['active'] != name:
                                    topic, topic_offset = name.split('_')
                                    with self.get_topic_lock(topic):
                                        topic_offset = int(topic_offset)
                                        del self.segment_files[topic][topic_offset]
                                        offsets = list(self.segment_files[topic].keys())
                                        offsets.remove('active')
                                        offsets.sort(reverse=True)
                                        self.segment_files[topic]['offsets'] = offsets
                                        for ofset in self.offsets[topic].keys():
                                            if ofset < topic_offset:
                                                del self.offsets[topic][ofset]
                                    os.remove(fpath)
                                    logging.info(f"DELETED FILE {fmodified} {fpath}")
                                else:
                                    logging.info(f"Not deleting active topic file: {fpath}")
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
        offsets = self.segment_files.get(topic, {}).get('offsets', [])
        topic_file = None
        file_offset = offset
        for ofset in offsets:
            if offset >= ofset:
                topic_file = self.segment_files[topic][ofset]
                file_offset = offset - ofset
                break
        # print(offsets, offset)
        if topic_file:
            path = os.path.join(self.topic_root, topic_file)
            if topic not in self.offsets:
                # logging.info('sening')
                conn.sendall(b' ')
                return
            msg_end = self.offsets.get(topic).get(offset, None)
            if msg_end is None:
                count = None
            else:
                count = msg_end - offset
            with open(path, 'rb') as rf:
                ## socket.sendfile API
                sent_count = conn.sendfile(rf, offset=file_offset, count=count)
                # logging.info(f'sent {sent_count} bytes, {offset}, {count}')
                return
        else:
            # logging.info('sening')
            conn.sendall(b' ')


    def write_to_topic(self, topic, bytes):
        try:
            with self.get_topic_lock(topic):
                if topic not in self.offsets:
                    self.offsets[topic] = ThreadSafeDict()
                    self.offsets[topic]['offset'] = 0
                topic_offsets = self.offsets[topic]
                previous_offset = topic_offsets['offset']

                if topic not in self.segment_files:
                    self.segment_files[topic] = ThreadSafeDict()
                    self.segment_files[topic][0] = f"{topic}_0"
                    self.segment_files[topic]['offsets'] = [0]
                    self.segment_files[topic]['active'] = f"{topic}_0"
                path = os.path.join(self.topic_root, self.segment_files[topic]['active'])
                file_desc = os.open(path, os.O_RDWR | os.O_CREAT)
                current_file_offset = previous_offset - self.segment_files[topic]['offsets'][0]
                os.pwrite(file_desc, bytes, current_file_offset)     
                os.close(file_desc)

                topic_offsets[previous_offset] = previous_offset + len(bytes)
                topic_offsets['offset'] = previous_offset + len(bytes)
                # logging.info(previous_offset)
                return previous_offset
        except Exception as e:
            print(f'Error in writing to topic {topic}: {e}')
            raise e

    
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
