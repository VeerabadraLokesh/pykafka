import os


# KAFKA_STORAGE_PATH = os.path.join(os.path.expanduser('~'), 'kafka_data')
KAFKA_STORAGE_PATH = os.path.join(os.getcwd(), 'kafka_data')

## Retention in seconds
# TOPIC_RETENTION_SLA = 600           # 600 seconds
TOPIC_RETENTION_SLA = 7 * 86400   # 7 days

DELETE_FILES_INTERVAL = 600          ## Delete files every 10 minutes

# MAX_SEGMENT_FILE_SIZE = 10 * 1024           ## 10 KB per segment file
MAX_SEGMENT_FILE_SIZE = 1024 * 1024 * 1024   ## 1 GiB per segment file

TOPIC_NAME_LENGTH = 5

ZOOKEEPER_SERVICE = "localhost:2181"

BROKER_SOCKET_HOST = "0.0.0.0"

CLIENTS_PORT = 9092

# PRODUCER_PORT = 9092

# CONSUMER_PORT = 9093


## Number of messages (>=1) on queue before the broker flushes the data to disk
FLUSH_MESSAGE_COUNT = 100
## Timeout for flushing messages to disk (>=0.1)
FLUSH_MESSAGE_TIMEOUT = 1

## 8 MiB ## Needs breaking msg into multiple packets and rebuilding
# BYTES_PER_MESSAGE = 8 * 1024 * 1024

## Max bytes per message
BYTES_PER_MESSAGE = 65476   ## 6 bytes for command and topic name; so 65470 max bytes per message

BROKER_CONNECTIONS = 1000

BOOTSTRAP_SERVERS="localhost:9092"

TEST_MESSAGE_COUNT = 100_000
TEST_MESSAGE_SIZE = 32768       ## 32 KiB

# TEST_MESSAGE_COUNT = 1_000

USE_ZOOKEPER_FOR_PULLING = False

### Set it to True to log errors about gaps in zookeeper updates (Only for test_consumer.py)
ENABLE_MSG_STATS = True

ENABLE_DEBUGGING_ZOOKEEPER = False
