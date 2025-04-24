from socket import SHUT_RD
from protocol.socket import (
    CsvTransferListener,
    CsvTransferStream,
    MSG_BATCH,
    MSG_FIN,
    MSG_ERR,
)
from protocol.sanitize import lines_to_sanitize, fin_to_sanitize
from protocol.sink import *
from rabbit.broker import Broker
from threading import Thread
import logging
import signal


def handle_result(conn: CsvTransferStream, ch, method, properties, body):
    kind, query, data = read_delivery_with_query(body)
    if kind == BATCH:
        batch = Batch.decode(data)
        data = batch.to_result(query)
        conn.send(data)

    elif kind == EOF:
        eof = Eof.decode(data)
        data = eof.to_result(query)
        conn.send(data)
        logging.info(f"Query {query} has been succesfully processed")

    elif kind == ERROR:
        error = Error.decode(data)
        data = error.to_result(query)
        conn.send(data)
        logging.info(f"There has been an error with query {query}")

    else:
        logging.error(f"Received an unknown data kind {kind}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


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
        conn = self._accept_new_conn()
        if not conn:
            return

        self._lis.close()

        # Background process to handle the client
        client_handler = Thread(
            target=self._handle_client,
            name="client handler process",
            args=(conn,),
        )
        client_handler.start()
        self._broker.consume("", handle_result, conn)

        client_handler.join()

        conn.close()
        self._broker.close()

    def _handle_client(self, conn: CsvTransferStream):
        for _ in range(3):
            filename = conn.resource()
            logging.info(f"Receiving {filename}")

            while True:
                msg = conn.recv()
                if msg.kind == MSG_FIN:
                    logging.info(f"{filename} was successfully received")
                    body = fin_to_sanitize(msg.data)
                    self._broker.publish(routing_key=filename + "-0", body=body)
                    break

                elif msg.kind == MSG_BATCH:
                    body = lines_to_sanitize(msg.data)
                    self._broker.publish(routing_key=filename + "-0", body=body)

                elif msg.kind == MSG_ERR:
                    logging.critical("An error occurred, exiting...")
                    return 1

                else:
                    logging.critical(f"An unknown msg kind was received {msg.kind}")
                    return 1

        logging.info("all files were succesfully received")

    def _accept_new_conn(self):
        logging.info(f"Waiting for connections...")

        try:
            conn, addr = self._lis.accept()
        except OSError:
            return None

        logging.info(f"Got a new connection from {addr}")
        return conn
