# Worker

## Variables
```sh
# .env
RABBIT_URL=amqp://guest:guest@rabbitmq:5672/
LOG_LEVEL=DEBUG | INFO | NOTICE | WARNING | ERROR | CRITICAL

# Docker Compose

# Input
INPUT_EXCHANGE_NAME=%string
INPUT_EXCHANGE_TYPE=direct | fanout | topic | headers
INPUT_QUEUE_NAMES=%[string]
INPUT_QUEUE_KEYS=%[string]

# Output
OUTPUT_EXCHANGE_NAME=%string
OUTPUT_EXCHANGE_TYPE=direct | fanout | topic | headers
OUTPUT_QUEUE_NAMES=%[string,]
OUTPUT_QUEUE_KEYS=%[string,]

# Worker
SELECT=%[string,]
```
