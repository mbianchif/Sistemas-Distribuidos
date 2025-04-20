import pika
import logging


class Broker:
    def __init__(self):
        logging.getLogger("pika").setLevel(logging.WARNING)
        self._exchange_name = "gateway"
        exchange_type = "direct"
        qs = ["movies", "credits", "ratings"] # Same as routing keys

        try:
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            self._chan = self._conn.channel()

            self._chan.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=exchange_type,
                durable=True,
            )

            for queue in qs:
                self._chan.queue_declare(queue=queue)
                self._chan.queue_bind(
                    exchange="gateway",
                    queue=queue,
                    routing_key=queue,
                )

        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise

    def publish(self, routing_key: str, body: str):
        try:
            self._chan.basic_publish(exchange=self._exchange_name, routing_key=routing_key, body=body)
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise

    def close(self):
        if self._chan and self._chan.is_open:
            try:
                self._chan.close()
            except:
                pass
        if self._conn and self._conn.is_open:
            self._conn.close()
