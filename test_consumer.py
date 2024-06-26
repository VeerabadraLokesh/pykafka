
import time
import logging
import numpy as np
import matplotlib.pyplot as plt

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

    message_timestamps = [[0, start_time]]

    message_index = 0
    for message in kafka_consumer.createMessageStreams():
        if message != b' ':
            # message_times[message_index] = time.perf_counter()
            total_bytes += len(message)
            if message_index % 100 == 99:
                logging.info(f"Received {(message_index+1)} in {time.perf_counter() - start_time:.3f} seconds")
                message_timestamps.append([message_index, time.perf_counter()])
            message_index += 1
            if message_index == S.TEST_MESSAGE_COUNT:
                break
    time_taken = time.perf_counter() - start_time

    tmb = total_bytes/1024/1024
    mibps = tmb / time_taken

    logging.info(f"Received {S.TEST_MESSAGE_COUNT} in {time_taken:.3f} seconds")
    logging.info(f"{tmb} MiB received in {time_taken:.3f} seconds. Rate: {mibps:.3f} MiB/s")
    
    message_timestamps = np.array(message_timestamps)

    message_timestamps[:, 1] -= message_timestamps[0, 1]

    try:
        plt.figure()
        plt.plot(message_timestamps[:,0], message_timestamps[:,1])
        plt.xlabel('Messages delivered')
        plt.ylabel('seconds')
        plt.title(f"Reading {S.TEST_MESSAGE_COUNT} - {(S.TEST_MESSAGE_SIZE/1024)} KiB messages - {mibps:.2f} MiBps")
        plt.savefig(f"{int(time.time()*100_000_000)}.svg")
        plt.show()
    except:
        pass
