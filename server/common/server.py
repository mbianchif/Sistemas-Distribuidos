from collections import defaultdict
import signal
import logging
import multiprocessing as mp
from common.protocol import BetSockListener, BetSockStream, KIND_BATCH, KIND_CONFIRM
from common.utils import has_won, load_bets, store_bets


class Server:
    def __init__(self, port, listen_backlog, nclients):
        self._listener = BetSockListener.bind("", port, listen_backlog)
        self._bets_file_lock = mp.Lock()
        self._shutdown = mp.Event()
        self._nclients = nclients

        def term_handler(_signum, _stacktrace):
            self._shutdown.set()

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        agencies = {}
        children = []

        while not self._shutdown and len(agencies) < self._nclients:
            stream = self._accept_new_connection()
            agencies[stream.id] = stream
            child = mp.Process(
                name=str(stream.id),
                target=self._handle_client_connection,
                args=(stream, self._shutdown, self._bets_file_lock),
            )
            children.append(child)
            child.start()

        self._listener.close()
        for child in children:
            child.join()

        if not self._shutdown.is_set():
            logging.info("action: sorteo | result: success")
            winners_counts = defaultdict(list)

            for bet in load_bets():
                if has_won(bet):
                    winners_counts[bet.agency].append(int(bet.document))

            for agency, stream in agencies.items():
                stream.send_winner_count(winners_counts[agency])

        for stream in agencies.values():
            stream.close()

    def _handle_client_connection(self, client, shutdown, file_lock):
        while not shutdown.is_set():
            msg = client.recv()

            if msg.kind == KIND_CONFIRM:
                logging.info(f"action: confirmacion_recibida | result: success")
                break

            if msg.kind == KIND_BATCH:
                logging.info(
                    f"action: apuesta_recibida | result: success | cantidad: {len(msg.data)}"
                )

                file_lock.acquire()
                store_bets(msg.data)
                file_lock.release()

    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
