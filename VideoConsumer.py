
import numpy as np
import cv2

from KafkaConsumer import KafkaConsumer

import settings as S


if __name__ == "__main__":


    topic = 'VIDEO'

    image_shape = (1080, 1920, 3)

    total_bytes = 1080 * 1920 * 3

    kafka_consumer = KafkaConsumer(bootstrap_servers=S.BOOTSTRAP_SERVERS, topic=topic)

    image_bytes = 0
    
    for message in kafka_consumer.createMessageStreams():
        if len(message) > 0 and message != b' ':
            image_bytes = message
            img = np.frombuffer(image_bytes, dtype=np.uint8).reshape(image_shape)
            cv2.imshow(winname="video", mat=img)
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
    