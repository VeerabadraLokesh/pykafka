

import socket
import time
import os

import settings as S


if __name__ == "__main__":

    HOST = "127.0.0.1"
    PORT = 18735

    # create socket and listen
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(S.BROKER_CONNECTIONS)
    print(f"Listening on {HOST}:{PORT}")

    try:
        # accept connection
        conn, addr = s.accept()
        print(f"Accepted connection from {HOST}:{PORT}")

        cmd = ""
        while True:
            print('waiting for data')
            data = conn.recv(S.BYTES_PER_MESSAGE)
            time.sleep(1)
            if not data:
                break
            print(data.decode())
            # conn.sendall(b"OK")  This line fixes the problem!
            # conn.send(b"Hello")
            path = os.path.join('test.txt')
            # file_size = os.path.getsize(path)
            # file_desc = os.open(path, os.O_RDONLY)
            # conn.sendfile(file_desc)
            # os.close(file_desc)
            with open(path, 'rb') as rf:
                conn.sendfile(rf)

        conn.sendall(b"Finished")
    finally:
        s.close()
