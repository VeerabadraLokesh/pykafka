import os


KAFKA_STORAGE_PATH = os.path.join(os.path.expanduser('~'), 'kafka_data')

## Retention in seconds
TOPIC_RETENTION_SLA = 600           # 600 seconds
# TOPIC_RETENTION_SLA = 7 * 86400   # 7 days

DELETE_FILES_INTERVAL = 60          ## Delete files every minute

MAX_SEGMENT_FILE_SIZE = 100 * 1024 * 1024   ## 100 MiB per segment file

TOPIC_NAME_LENGTH = 5

ZOOKEEPER_SERVICE = "localhost:2181"

BROKER_SOCKET_HOST = "0.0.0.0"

CLIENTS_PORT = 9092

PRODUCER_PORT = 9092

CONSUMER_PORT = 9093


FLUSH_MESSAGE_COUNT = 100
FLUSH_MESSAGE_TIMEOUT = 1

## 8 MiB
BYTES_PER_MESSAGE = 8 * 1024 * 1024

BROKER_CONNECTIONS = 1000

BOOTSTRAP_SERVERS="localhost:9092"
