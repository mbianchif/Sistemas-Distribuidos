from protocol.socket import CsvTransferStream
from rabbit.broker import Broker


class Robin:
    def __init__(self, broker: Broker, fmt: str, output_copies: int):
        self._broker = broker
        self._fmt = fmt
        self._output_copies = output_copies
        self._i = 0

    def _next_key(self) -> str:
        key = self._fmt.format(self._i)
        self._i = (self._i + 1) % self._output_copies
        return key

    def direct(self, body):
        key = self._next_key()
        self._broker.publish(key, body)

    def broadcast(self, body):
        for i in range(self._output_copies):
            key = self._fmt.format(i)
            self._broker.publish(key, body)


class Mailer:
    def __init__(self, config):
        self._broker = Broker(config)
        self._config = config
        self._senders: list[Robin] | None = None
        self._filename_2_id = {
            "movies": 0,
            "credits": 1,
            "ratings": 2,
        }

    def _init_senders(self, output_fmts: list[str]):
        output_copies = self._config.outputCopies
        senders = []

        for i in range(len(output_copies)):
            sender = Robin(self._broker, output_fmts[i], output_copies[i])
            senders.append(sender)

        return senders

    def initialize(self) -> dict[int, int]:
        input_copies, output_fmts = self._broker.initialize()
        self._senders = self._init_senders(output_fmts)
        return input_copies

    def consume(self, callback, conn: CsvTransferStream):
        return self._broker.consume("", callback, conn)

    def publish_batch(self, filename: str, body: bytearray | bytes):
        if self._senders is None:
            raise ValueError("didn't initialize Mailer")

        self._senders[self._filename_2_id[filename]].direct(body)

    def publish_eof(self, filename: str, body: bytearray | bytes):
        if self._senders is None:
            raise ValueError("didn't initialize Mailer")

        self._senders[self._filename_2_id[filename]].broadcast(body)

    def close(self):
        self._broker.close()
