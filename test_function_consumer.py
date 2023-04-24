
import time
import logging

import pickle
import marshal
import types

from KafkaConsumer import KafkaConsumer
import settings as S



if __name__ == "__main__":


    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)

    topic = 'FUNCS'

    kafka_consumer = KafkaConsumer(bootstrap_servers=S.BOOTSTRAP_SERVERS, topic=topic)

    for message in kafka_consumer.createMessageStreams():
        if message != b' ':
            unpickeld_obj = pickle.loads(message)
            func = types.FunctionType(marshal.loads(unpickeld_obj['func']), globals(), unpickeld_obj['func_name'])
            func(*unpickeld_obj['inputs'])

