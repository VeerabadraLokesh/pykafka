
import time
import logging

from KafkaConsumer import KafkaConsumer
import settings as S



if __name__ == "__main__":


    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)

    topic = 'STATS'

    kafka_consumer = KafkaConsumer(bootstrap_servers=S.BOOTSTRAP_SERVERS, topic=topic)

    ### Uncomment to log errors about gaps in zookeeper updates
    # kafka_consumer.enable_error_logs = True

    # message_times = [0] * S.TEST_MESSAGE_COUNT
    total_bytes = 0
    start_time = time.perf_counter()

    message_index = 0
    for message in kafka_consumer.createMessageStreams():
        if message != b' ':
            # message_times[message_index] = time.perf_counter()
            total_bytes += len(message)
            if message_index % 100 == 99:
                logging.info(f"Received {(message_index+1)} in {time.perf_counter() - start_time} seconds")
            message_index += 1
            if message_index == S.TEST_MESSAGE_COUNT:
                break
    time_taken = time.perf_counter() - start_time

    tmb = total_bytes/1024/1024
    mibps = tmb / time_taken

    logging.info(f"Received {S.TEST_MESSAGE_COUNT} in {time_taken} seconds")
    logging.info(f"{tmb} MiB received in {time_taken:.3f} seconds. Rate: {mibps:.3f} MiB/s")
