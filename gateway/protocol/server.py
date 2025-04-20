from socket import SHUT_RD
from protocol.socket import (
    CsvTransferListener,
    CsvTransferStream,
    MSG_BATCH,
    MSG_FIN,
    MSG_ERR,
)
from protocol.sanitize import lines_to_sanitize, fin_to_sanitize
from rabbit.broker import Broker
import logging
import signal


class Server:
    def __init__(self, host: str, port: int, backlog: int = 0):
        self._lis = CsvTransferListener.bind(host, port, backlog)
        self._shutdown = False
        self._broker = Broker()

        def term_handler(_signum, _stacktrace):
            self._shutdown = True
            self._lis.shutdown(SHUT_RD)

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        conn = self._accept_new_conn()
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
                    body = fin_to_sanitize(msg.data)
                    self._broker.publish(routing_key=filename, body=body)
                    break

                elif msg.kind == MSG_BATCH:
                    body = lines_to_sanitize(msg.data)
                    self._broker.publish(routing_key=filename, body=body)

                elif msg.kind == MSG_ERR:
                    logging.critical("An error occurred, exiting...")
                    stream.close()
                    return 1

                else:
                    logging.critical(f"An unknown msg kind was received {msg.kind}")
                    stream.close()
                    return 1

    def _accept_new_conn(self):
        logging.info(f"Waiting for connections...")

        try:
            conn, addr = self._lis.accept()
        except OSError:
            return None

        logging.info(f"Got a new connection from {addr}")
        return conn
