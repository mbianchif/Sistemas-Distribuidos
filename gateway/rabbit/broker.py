import os
import pika
import logging

class Broker:
    def __init__(self):
        self._output_exchange_name = os.getenv("OUTPUT_EXCHANGE_NAME", "")
        self._output_exchange_type = os.getenv("OUTPUT_EXCHANGE_TYPE", "")
        self._output_exchange_queues = os.getenv("OUTPUT_QUEUES", "")
        self._output_exchange_keys = os.getenv("OUTPUT_QUEUE_KEYS", "").split(",")

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

            self.channel = connection.channel()

            self.channel.exchange_declare(exchange=self._output_exchange_name, exchange_type=self._output_exchange_type, durable=True)

            for i, queue in enumerate(self._output_exchange_queues.split(",")):
                self.channel.queue_declare(queue=queue)
                self.channel.queue_bind(exchange=self._output_exchange_name, queue=queue, routing_key=self._output_exchange_keys[i])

        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise
    
    def publish(self, routing_key: str, body: str):
        try:
            self.channel.basic_publish(exchange=self._output_exchange_name, routing_key=routing_key, body=body)
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise
    