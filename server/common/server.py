import signal
import logging
from multiprocessing import Barrier, Lock, Process
from time import sleep
from common.protocol import BetSockListener, BetSockStream, KIND_BATCH, KIND_CONFIRM
from common.utils import has_won, load_bets, store_bets


class Server:
    def __init__(self, port, listen_backlogging, nclients):
        self._listener = BetSockListener.bind("", port, listen_backlogging)
        self._barrier = Barrier(nclients)
        self._nclients = nclients
        self._file_lock = Lock()
        self._shutdown = False

        def term_handler(_signum, _stacktrace):
            self._shutdown = True

        signal.signal(signal.SIGTERM, term_handler)

    def run(self):
        clients = []

        while not self._shutdown and len(clients) < self._nclients:
            stream = self._accept_new_connection()
            child = Process(
                name=str(stream.id),
                target=self._handle_client_connection,
                args=(stream, self._file_lock, self._barrier),
            )

            clients.append((child, stream))
            child.start()

        self._listener.close()
        for child, stream in clients:
            child.join()
            stream.close()

        # Esto está para garantizar que el proceso del server termine después que
        # el de todos los clientes. Hay un test que cuenta la cantidad de exits
        # leídos en los logs, si el server termina antes que alguno de los clientes
        # es muy probable que no haya leído la cantidad de ganadores de ese cliene.
        sleep(5)

    def _handle_client_connection(self, client, file_lock, barrier):
        while True:
            msg = client.recv()

            if msg.kind == KIND_CONFIRM:
                logging.info(f"action: confirmacion_recibida | result: success")
                self._send_winners(client, file_lock, barrier)
                break

            if msg.kind == KIND_BATCH:
                logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(msg.data)}")
                with file_lock:
                    store_bets(msg.data)

    def _send_winners(self, client, file_lock, barrier):
        if barrier.wait() == 0:
            logging.info("action: sorteo | result: success")

        winners = []
        with file_lock:
            for bet in load_bets():
                if has_won(bet) and bet.agency == client.id:
                    winners.append(int(bet.document))

        client.send_winner_count(winners)

    def _accept_new_connection(self) -> BetSockStream:
        logging.info("action: accept_connections | result: in_progress")
        conn, addr = self._listener.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return conn
