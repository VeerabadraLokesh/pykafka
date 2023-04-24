
import os
import logging
import time
import pickle
import marshal
import types
from KafkaProducer import KafkaProducer
import settings as S



def factorial(x):
    if x > 64:
        print(f"input {x} too big for factorial")
        return
    f = 1
    for i in range(2, x+1):
        f = f*i
    print(f"Factorial of {x} is {f}")


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)
    
    topic = "FUNCS"

    req_obj = {
        "func": marshal.dumps(factorial.__code__),
        "func_name": factorial.__name__,
        "inputs": [64]
    }

    pickled_obj = pickle.dumps(req_obj)

    # print(type(pickled_obj))

    # unpickeld_obj = pickle.loads(pickled_obj)
    # func = types.FunctionType(marshal.loads(unpickeld_obj['func']), globals(), unpickeld_obj['func_name'])
    # func(*unpickeld_obj['inputs'])

    kafka_producer = KafkaProducer(bootstrap_servers=S.BOOTSTRAP_SERVERS)
    kafka_producer.send(topic, pickled_obj)
    kafka_producer.wait()
    