import signal
import logging
from common.protocol import BetSockListener, BetSockStream
from common.utils import store_bets


class Server:
    def __init__(self, port, listen_backlog, nclients):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._nclients = nclients
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True
            self._listener.close()

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        served = 0
        while not self._shutdown and served < self._nclients:
            try:
                stream = self._accept_new_connection()
                self._handle_client_connection(stream)
                served += 1
            except OSError as e:
                logging.error(f"action: run_server | result: fail | error: {e}")
                break

    def _handle_client_connection(self, client_sock: BetSockStream):
        try:
            bets = client_sock.recv()
            store_bets(bets)
            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
        except OSError as _:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bets)}")
        finally:
            client_sock.close()

    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
