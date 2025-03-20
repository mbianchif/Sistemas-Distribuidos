import signal
import logging
from common.protocol import BetSockListener, BetSockStream, MessageKind
from common.utils import has_won, load_bets, store_bets

N_AGENCIES = 5

class Server:
    def __init__(self, port, listen_backlog):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        agencies = {}

        while not self._shutdown and len(agencies) < N_AGENCIES:
            stream = self._accept_new_connection()
            self._handle_client_connection(stream)
            agencies[stream.id] = stream

        if not self._shutdown:
            logging.info("action: sorteo | result: success")
            winners_counts = [0] * N_AGENCIES

            for bet in load_bets():
                if has_won(bet):
                    winners_counts[bet.agency] += 1

            for agency, stream in agencies.items():
                stream.send_winner_count(winners_counts[agency])

        self._listener.close()

    def _handle_client_connection(self, client_sock: BetSockStream):
        while True:
            msg = client_sock.recv()
            if msg.kind == MessageKind.CONFIRM:
                break
            else:
                bets = msg.data
                store_bets(bets)
                logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")


    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
