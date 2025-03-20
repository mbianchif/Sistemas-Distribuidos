from enum import Enum
import socket
from typing import Any
from common.utils import Bet

BET_DELIMITER = ";"
BATCH_SIZE_SIZE = 4
BATCH_COUNT_SIZE = 4
ID_SIZE = 1
KIND_SIZE = 1
WINNER_COUNT_SIZE = 4


class MessageKind(Enum):
    BATCH = 0
    CONFIRM = 1


class Message:
    def __init__(self, kind: MessageKind, data: Any):
        self.kind = kind
        self.data = data


def _recv_all(skt: socket.socket, n: int) -> bytearray:
    data = bytearray()

    while len(data) < n:
        read = skt.recv(n - len(data), socket.MSG_WAITALL)
        if not read:
            raise OSError("couldn't recv whole message")

        data.extend(read)

    return data


def _send_all(skt: socket.socket, data: bytes):
    total_sent = 0

    while total_sent < len(data):
        sent = skt.send(data[total_sent:])
        if sent == 0:
            raise OSError("couldn't send message")

        total_sent += sent


class BetSockStream:
    def __init__(self, skt: socket.socket, id: int):
        self._skt = skt
        self.id = id

    def _recv_batch(self) -> list[Bet]:
        nbatches_bytes = _recv_all(self._skt, BATCH_COUNT_SIZE)
        nbatches = int.from_bytes(nbatches_bytes, "big")
        bets = []

        for _ in range(nbatches):
            batch_size_bytes = _recv_all(self._skt, BATCH_SIZE_SIZE)
            batch_size = int.from_bytes(batch_size_bytes, "big")

            batch_bytes = _recv_all(self._skt, batch_size)
            for bet_bytes in batch_bytes.split(BET_DELIMITER.encode()):
                bet = Bet.from_bytes(bet_bytes)
                bets.append(bet)

        return bets

    def recv(self):
        kind_bytes = _recv_all(self._skt, KIND_SIZE)
        kind = int.from_bytes(kind_bytes, "big")

        if kind == MessageKind.BATCH:
            batch = self._recv_batch()
            return Message(MessageKind.BATCH, batch)
        if kind == MessageKind.CONFIRM:
            return Message(MessageKind.CONFIRM, None)

        raise ValueError(f"invalid message kind {kind}")

    def send_winner_count(self, n: int):
        count_bytes = n.to_bytes(WINNER_COUNT_SIZE, "big")
        _send_all(self._skt, count_bytes)

    def close(self):
        self._skt.close()


class BetSockListener:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def bind(cls, host: str, port: int, backlog: int = 0):
        """
        Instanciates a new BetSockListener and binds it to the given address
        """
        self = cls(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._skt.bind((host, port))
        self._skt.listen(backlog)
        return self

    def accept(self) -> tuple[BetSockStream, "socket._RetAddress"]:
        """
        Blocks the calling thread until a new connection arrives
        """
        skt, addr = self._skt.accept()
        id_bytes = _recv_all(skt, ID_SIZE)
        id = int.from_bytes(id_bytes, "big")
        return BetSockStream(skt, id), addr

    def close(self):
        self._skt.close()
