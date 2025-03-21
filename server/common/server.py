from collections import defaultdict
import signal
import logging
from common.protocol import BetSockListener, BetSockStream, KIND_BATCH, KIND_CONFIRM
from common.utils import has_won, load_bets, store_bets

class Server:
    def __init__(self, port, listen_backlog, nclients):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._nclients = nclients
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        agencies = {}

        while not self._shutdown and len(agencies) < self._nclients:
            stream = self._accept_new_connection()
            self._handle_client_connection(stream)
            agencies[stream.id] = stream

        if not self._shutdown:
            logging.info("action: sorteo | result: success")
            winners_counts = defaultdict(int)

            for bet in load_bets():
                if has_won(bet):
                    winners_counts[bet.agency] += 1

            for agency, stream in agencies.items():
                stream.send_winner_count(winners_counts[agency])

        for stream in agencies.values():
            stream.close()

        self._listener.close()

    def _handle_client_connection(self, client_sock: BetSockStream):
        while True:
            msg = client_sock.recv()

            if msg.kind == KIND_CONFIRM:
                logging.info(f"action: confirmacion_recibida | result: success")
                break

            if msg.kind == KIND_BATCH:
                bets = msg.data
                store_bets(bets)
                logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")


    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
