FROM ubuntu:latest
ENV DEBIAN_FRONTEND noninteractive


RUN apt update && apt install -y make python3 python3-pip iproute2 python-is-python3

WORKDIR /opt/docker/
COPY requirements.txt /opt/docker/
RUN python3 -m pip install -r requirements.txt

COPY . /opt/docker/

CMD bash