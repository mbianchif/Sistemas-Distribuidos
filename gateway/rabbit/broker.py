import pika
import logging


class Broker:
    def __init__(self, config):
        logging.getLogger("pika").setLevel(logging.WARNING)
        
        self._outputexchange_name = config.outputExchangeName
        self._input_queue_name = config.inputQueueNames[0]

        try:
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            self._chan = self._conn.channel()
            
            self.inputQueues = self._declareSide(config.inputExchangeName, config.inputExchangeType, config.inputQueueNames, config.inputQueueKeys)
            self.outputQueues = self._declareSide(self._outputexchange_name, config.outputExchangeType, config.outputQueueNames, config.outputQueueKeys)
        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise

    def _declareSide(self, exchange_name, exchange_type, queues_name, queues_keys):
        self._chan.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=True,
        )

        colas = []
        for i, queue in enumerate(queues_name):
            self._chan.queue_declare(queue=queue)
            cola = self._chan.queue_bind(
                exchange=exchange_name,
                queue=queue,
                routing_key=queues_keys[i],
            )
            colas.append(cola)

        return colas

    def publish(self, routing_key: str, body: str):
        try:
            self._chan.basic_publish(exchange=self._outputexchange_name, routing_key=routing_key, body=body)
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise
    
    def consume(self, consumer, callback):
        try:
            self._chan.basic_consume(
                    queue=self._input_queue_name, 
                    on_message_callback=callback,
                    auto_ack=False,
                    exclusive=False,
                    consumer_tag=consumer,
                    arguments=None,
                )
            self._chan.start_consuming()
        except Exception as e:
            logging.critical(f"Failed to consume message {e}")
            raise
        finally:
            self._chan.stop_consuming()
            self._chan.close()
            logging.critical(f"Failed to consume message {e}")

    def close(self):
        if self._chan and self._chan.is_open:
            try:
                self._chan.close()
            except:
                pass
        if self._conn and self._conn.is_open:
            self._conn.close()
