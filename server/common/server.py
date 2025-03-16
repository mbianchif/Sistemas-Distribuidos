import signal
import logging
from common.protocol import BetSockListener, BetSockStream
from common.utils import Bet, store_bets


class Server:
    def __init__(self, port, listen_backlog):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        while not self._shutdown:
            logging.info(f"action: new_conn | result: awaiting")
            stream = self._accept_new_connection()
            try:
                logging.info(f"action: new_conn | result: success")
                self._handle_client_connection(stream)
            finally:
                stream.close()

        self._listener.close()

    def _handle_client_connection(self, client_sock: BetSockStream):
        try:
            logging.info(f"action: recv | result: awaiting")
            msgs = client_sock.recv()
            bets = []

            for msg in msgs:
                bet = Bet(
                    msg._agency,
                    msg._name,
                    msg._surname,
                    msg._id,
                    msg._birthdate,
                    msg._number,
                )
                bets.append(bet)

            store_bets(bets)
            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
        except OSError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bets)}")

    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
