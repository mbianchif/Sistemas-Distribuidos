import pika
import logging
from functools import partial
from ..config.config import Config


class Broker:
    def __init__(self, config):
        logging.getLogger("pika").setLevel(logging.WARNING)
        self._outputexchange_name = config.outputExchangeName
        self._input_queue_name = config.inputQueueNames[0]

        try:
            self._recvconn = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            self._sendconn = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )

            self._recvchan = self._recvconn.channel()
            self._sendchan = self._sendconn.channel()

            self.inputQueues = self._declare_side(
                self._recvchan,
                config.inputExchangeName,
                config.inputExchangeType,
                config.inputQueueNames,
                config.inputQueueKeys,
            )

            self.outputQueues = self._declare_side(
                self._sendchan,
                self._outputexchange_name,
                config.outputExchangeType,
                config.outputQueueNames,
                config.outputQueueKeys,
            )
        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise

    def _init_input(self, chan, config: Config):
        exchange_name = config.inputExchangeName
        q_name = config.inputQueueName
        
        chan.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=True,
        )

        chan.queue_declare(queue=q_name)
        return chan.queue_bind(
            exchange=exchange_name,
            exchange_type="direct",
            durable=True
        )

    def _init_output(self, chan, config: Config):
        exchange_name = config.outputExchangeName
        q_names = config.outputQueueNames
        q_copies = config.outputCopies

        chan.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=True,
        )

        for i in range(len(q_names)):
            q_fmt = q_names[i] + "-{}"
            for id in range(q_copies[i]):
                q_name = q_fmt.format(id)

                chan.queue_declare(queue=q_name)
                q = chan.queue_bind(
                    exchange=exchange_name,
                    exchange_type="direct",
                    durable=True,
                )



    def _declare_side(self, chan, exchange_name, exchange_type, queues_name, queues_keys):
        chan.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=True,
        )

        qs = []
        for i, queue in enumerate(queues_name):
            chan.queue_declare(queue=queue)
            q = chan.queue_bind(
                exchange=exchange_name,
                queue=queue,
                routing_key=queues_keys[i],
            )
            qs.append(q)

        return qs

    def publish(self, routing_key: str, body: str | bytes):
        try:
            self._sendchan.basic_publish(
                exchange=self._outputexchange_name,
                routing_key=routing_key,
                body=body,
            )
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise

    def consume(self, consumer, callback, conn):
        try:
            self._recvchan.basic_consume(
                queue=self._input_queue_name,
                on_message_callback=partial(callback, conn),
                auto_ack=False,
                exclusive=False,
                consumer_tag=consumer,
            )
            self._recvchan.start_consuming()
        except Exception as e:
            logging.critical(f"Failed to consume message {e}")
            raise
        finally:
            self._recvchan.stop_consuming()

    def close(self):
        if self._recvchan and self._recvchan.is_open:
            try:
                self._recvchan.close()
            except:
                pass
        if self._sendchan and self._sendchan.is_open:
            try:
                self._sendchan.close()
            except:
                pass
        if self._recvconn and self._recvconn.is_open:
            self._recvconn.close()
        if self._sendconn and self._sendconn.is_open:
            self._sendconn.close()
