import signal
import logging
from common.protocol import BetSockListener, BetSockStream
from common.utils import store_bets


class Server:
    def __init__(self, port, listen_backlog):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        while not self._shutdown:
            stream = self._accept_new_connection()
            try:
                self._handle_client_connection(stream)
            finally:
                stream.close()

        self._listener.close()

    def _handle_client_connection(self, client_sock: BetSockStream):
        try:
            bets = client_sock.recv()
            store_bets(bets)
            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
        except OSError as _:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bets)}")

    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
