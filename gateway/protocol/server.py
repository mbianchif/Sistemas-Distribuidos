from socket import SHUT_RD
from protocol.socket import (
    CsvTransferListener,
    CsvTransferStream,
    MSG_BATCH,
    MSG_FIN,
    MSG_ERR,
)
from rabbit.broker import Broker
import logging
import signal


class Server:
    def __init__(self, config):
        self._lis = CsvTransferListener.bind(config.host, config.port, config.backlog)
        self._shutdown = False
        self._broker = Broker(config)

        def term_handler(_signum, _stacktrace):
            self._shutdown = True
            self._lis.shutdown(SHUT_RD)

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        while not self._shutdown:
            conn = self._accept_new_conn()
            if conn is None:
                continue

            self._handle_client(conn)
            conn.close()

        self._lis.close()
        self._broker.close()

    def _handle_client(self, stream: CsvTransferStream):
        for _ in range(3):
            filename = stream.resource()
            logging.info(f"Receiving {filename}")

            while True:
                msg = stream.recv()
                if msg.kind == MSG_FIN:
                    logging.info(f"{filename} was successfully received")
                    break

                elif msg.kind == MSG_BATCH:
                    for line in msg.data:
                        self._broker.publish(routing_key=filename, body=line)

                elif msg.kind == MSG_ERR:
                    logging.critical("An error occurred, exiting...")
                    stream.close()
                    return 1

                else:
                    logging.critical(f"An unknown msg kind was received {msg.kind}")
                    stream.close()
                    return 1
        
        for _ in range(5):
            while True:
                #TODO: falta recibir el end del result para poder hacer el break
                logging.info("Waiting for results...")
                self._broker.consume(self._broker.inputQueues[0], "", self._handle_result)
            
    def _handle_result(self, ch, method, properties, body):
        logging.info(f"Received result: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _accept_new_conn(self):
        logging.info(f"Waiting for connections...")

        try:
            conn, addr = self._lis.accept()
        except OSError:
            return None

        logging.info(f"Got a new connection from {addr}")
        return conn
