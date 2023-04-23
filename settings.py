import os


KAFKA_STORAGE_PATH = os.path.join(os.path.expanduser('~'), 'kafka_data')

## Retention in seconds
# TOPIC_RETENTION_SLA = 600           # 600 seconds
TOPIC_RETENTION_SLA = 7 * 86400   # 7 days

DELETE_FILES_INTERVAL = 60          ## Delete files every minute

# MAX_SEGMENT_FILE_SIZE = 10 * 1024           ## 10 KB per segment file
MAX_SEGMENT_FILE_SIZE = 100 * 1024 * 1024   ## 100 MiB per segment file

TOPIC_NAME_LENGTH = 5

ZOOKEEPER_SERVICE = "localhost:2181"

BROKER_SOCKET_HOST = "0.0.0.0"

CLIENTS_PORT = 9092

# PRODUCER_PORT = 9092

# CONSUMER_PORT = 9093


FLUSH_MESSAGE_COUNT = 100
FLUSH_MESSAGE_TIMEOUT = 1

## 8 MiB ## Needs breaking msg into multiple packets and rebuilding
# BYTES_PER_MESSAGE = 8 * 1024 * 1024

## Max bytes per message
BYTES_PER_MESSAGE = 65476

BROKER_CONNECTIONS = 1000

BOOTSTRAP_SERVERS="localhost:9092"

TEST_MESSAGE_COUNT = 10_000
TEST_MESSAGE_SIZE = 32768       ## 32 KiB

# TEST_MESSAGE_COUNT = 1_000

USE_ZOOKEPER_FOR_PULLING = False

### Set it to True to log errors about gaps in zookeeper updates (Only for test_consumer.py)
ENABLE_DEBUGGING = False
