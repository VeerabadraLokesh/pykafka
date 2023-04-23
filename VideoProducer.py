
import os
import urllib.request
import cv2
import progressbar
import logging
import numpy as np

from KafkaProducer import KafkaProducer
import settings as S

pbar = None


def show_progress(block_num, block_size, total_size):
    global pbar
    if pbar is None:
        pbar = progressbar.ProgressBar(maxval=total_size)
        pbar.start()

    downloaded = block_num * block_size
    if downloaded < total_size:
        pbar.update(downloaded)
    else:
        pbar.finish()
        pbar = None

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%d-%m-%Y:%H:%M:%S',
                        level=logging.INFO)
    
    topic = "VIDEO"

    kafka_producer = KafkaProducer(bootstrap_servers=S.BOOTSTRAP_SERVERS)


    URL = "https://download.blender.org/demo/movies/BBB/bbb_sunflower_1080p_30fps_normal.mp4"
    VIDEO_FILE = "bbb_sunflower_1080p_30fps_normal.mp4"

    URL = "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4"
    VIDEO_FILE = "BigBuckBunny_320x180.mp4"

    URL = "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/1080/Big_Buck_Bunny_1080_10s_30MB.mp4"
    VIDEO_FILE = "Big_Buck_Bunny_1080_10s_30MB.mp4"

    URL = "https://upload.wikimedia.org/wikipedia/commons/f/f3/Big_Buck_Bunny_first_23_seconds_1080p.ogv"
    VIDEO_FILE = "Big_Buck_Bunny_first_23_seconds_1080p.ogv"

    if not os.path.isfile(VIDEO_FILE):
        logging.info(f"Dowloading video file {VIDEO_FILE}")
        urllib.request.urlretrieve(URL, VIDEO_FILE, show_progress) 
    
    vidcap = cv2.VideoCapture(VIDEO_FILE)
    success,image = vidcap.read()
    count = 0
    os.makedirs('images', exist_ok=True)
    while success:
        # cv2.imwrite("images/frame%d.jpg" % count, image)     # save frame as JPEG file    
        image_bytes = image.tobytes()  
        # print(image.shape, image.dtype, type(image), len(image_bytes))
        kafka_producer.send(topic=topic, message=image_bytes)
        logging.info(f"Sent {count} frames {len(image_bytes)}")
        success,image = vidcap.read()
        count += 1
        if count == 10:
            break
    kafka_producer.wait()