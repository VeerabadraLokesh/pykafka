

import socket
import time

import settings as S


class KafkaProducer:

    def __init__(self) -> None:
        pass


if __name__ == "__main__":

    time.sleep(1)

    HOST = "127.0.0.1"
    PORT = 18735

    # create socket and connect
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cs.connect((HOST, PORT))

    while True:
        # send data
        cs.sendall(b"123456")

        # wait for a result
        data = cs.recv(S.BYTES_PER_MESSAGE)
        print("result: ", data.decode())
        time.sleep(1)
        break
    cs.close()
