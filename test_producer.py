
import os
import logging
import time

from KafkaProducer import KafkaProducer
import settings as S

pbar = None


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)
    
    topic = "STATS"

    # TEST_MESSAGES = 100_000
    TEST_MESSAGES = S.TEST_MESSAGE_COUNT

    RANDOM_MESSAGES = 100
    MESSAGE_SIZE = S.TEST_MESSAGE_SIZE #1024

    random_messages = []

    ## https://stackoverflow.com/questions/7044044/an-efficient-way-of-making-a-large-random-bytearray
    for i in range(RANDOM_MESSAGES):
        random_messages.append(bytearray(os.urandom(MESSAGE_SIZE)))
    
    # print(len(random_messages))

    kafka_producer = KafkaProducer(bootstrap_servers=S.BOOTSTRAP_SERVERS)

    total_bytes = MESSAGE_SIZE * TEST_MESSAGES
    tmb = total_bytes/1024/1024

    start_time = time.perf_counter()
    for i in range(TEST_MESSAGES):
        kafka_producer.send(topic, random_messages[i % RANDOM_MESSAGES])
        if i % 10000 == 9999 or i == TEST_MESSAGES-1:
            logging.info(f"enqueued {(i+1)} messages")
    
    kafka_producer.wait()
    end_time = time.perf_counter()

    time_taken = end_time-start_time
    mibps = tmb / time_taken

    logging.info(f"Sent {TEST_MESSAGES} in {time_taken:.3f} seconds")
    logging.info(f"{tmb} MiB transferred in {time_taken:.3f} seconds. Rate: {mibps:.3f} MiB/s")
