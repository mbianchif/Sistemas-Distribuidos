import pika
import logging
from functools import partial



class Broker:
    def __init__(self, config):
        logging.getLogger("pika").setLevel(logging.WARNING)
        
        self._outputexchange_name = config.outputExchangeName
        self._input_queue_name = config.inputQueueNames[0]

        try:
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            self._chan = self._conn.channel()

            self.inputQueues = self._declare_side(config.inputExchangeName, config.inputExchangeType, config.inputQueueNames, config.inputQueueKeys)
            self.outputQueues = self._declare_side(self._outputexchange_name, config.outputExchangeType, config.outputQueueNames, config.outputQueueKeys)
        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise

    def _declare_side(self, exchange_name, exchange_type, queues_name, queues_keys):
        self._chan.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=True,
        )

        qs = []
        for i, queue in enumerate(queues_name):
            self._chan.queue_declare(queue=queue)
            q = self._chan.queue_bind(
                exchange=exchange_name,
                queue=queue,
                routing_key=queues_keys[i],
            )
            qs.append(q)

        return qs

    def publish(self, routing_key: str, body: str | bytes):
        try:
            self._chan.basic_publish(exchange=self._outputexchange_name, routing_key=routing_key, body=body)
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise
    
    def consume(self, consumer, callback, conn):
        try:
            self._chan.basic_consume(
                    queue=self._input_queue_name, 
                    on_message_callback=partial(callback, conn),
                    auto_ack=False,
                    exclusive=False,
                    consumer_tag=consumer,
                )
            self._chan.start_consuming()
        except Exception as e:
            logging.critical(f"Failed to consume message {e}")
            raise
        finally:
            self._chan.stop_consuming()

    def close(self):
        if self._chan and self._chan.is_open:
            try:
                self._chan.close()
            except:
                pass
        if self._conn and self._conn.is_open:
            self._conn.close()
