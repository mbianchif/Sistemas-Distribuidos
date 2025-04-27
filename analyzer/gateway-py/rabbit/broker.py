import pika
import logging
from functools import partial


class Broker:
    def __init__(self, config):
        logging.getLogger("pika").setLevel(logging.WARNING)
        self._config = config

    def initialize(self) -> tuple[dict[int, int], list[str]]:
        try:
            # Init input
            self._recvconn = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            self._recvchan = self._recvconn.channel()
            self._init_input()

            # Init output
            self._sendconn = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            self._sendchan = self._sendconn.channel()
            output_fmts = self._init_output()

            return {i + 1: copies for i, copies in enumerate(self._config.inputCopies)}, output_fmts

        except Exception as e:
            logging.critical(f"Failed to connect with RabbitMQ: {e}")
            raise

    def _init_input(self):
        exchange_name = self._config.inputExchangeName
        q_name = self._config.inputQueueName + f"-{self._config.id}"

        self._recvchan.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=True,
        )

        self._recvchan.queue_declare(queue=q_name)
        self._recvchan.queue_bind(
            exchange=exchange_name,
            queue=q_name,
            routing_key=q_name,
        )

    def _init_output(self) -> list[str]:
        exchange_name = self._config.outputExchangeName
        q_names = self._config.outputQueueNames
        q_copies = self._config.outputCopies

        self._sendchan.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=True,
        )

        output_fmts = []
        for i in range(len(q_names)):
            q_fmt = q_names[i] + "-{}"
            output_fmts.append(q_fmt)

            for id in range(q_copies[i]):
                q_name = q_fmt.format(id)

                self._sendchan.queue_declare(queue=q_name)
                self._sendchan.queue_bind(
                    exchange=exchange_name,
                    queue=q_name,
                    routing_key=q_name,
                )

        return output_fmts

    def publish(self, routing_key: str, body: str | bytes):
        try:
            self._sendchan.basic_publish(
                exchange=self._config.outputExchangeName,
                routing_key=routing_key,
                body=body,
            )
        except Exception as e:
            logging.critical(f"Failed to publish message {e}")
            raise

    def consume(self, consumer, callback, conn):
        try:
            self._recvchan.basic_consume(
                queue=self._config.inputQueueName + "-0",
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
