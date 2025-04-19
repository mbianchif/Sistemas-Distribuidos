# Worker

## Variables
```sh
# .env
RABBIT_URL=amqp://guest:guest@rabbitmq:5672/
LOG_LEVEL=DEBUG | INFO | NOTICE | WARNING | ERROR | CRITIAL

# Docker Compose

# Input
INPUT_EXCHANGE_NAME=%string
INPUT_EXCHANGE_TYPE=direct | fanout | topic | headers
INPUT_QUEUES=%[string]
INPUT_QUEUE_KEYS=%[string]

# Output
OUTPUT_EXCHANGE_NAME=%string
OUTPUT_EXCHANGE_TYPE=direct | fanout | topic | headers
OUTPUT_QUEUEES=%[string,]
OUTPUT_QUEUE_KEYS=%[string,]

# Worker
SELECT=%[string,]
```
