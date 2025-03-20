import socket
from common.utils import Bet

BET_SIZE_SIZE = 4


class BetSockStream:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def connect(cls, host: str, port: int):
        """
        Instanciates a new BetSockStream connected to the given address
        """
        self = cls(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._skt.connect((host, port))
        return self

    def peer_addr(self) -> "socket._RetAddress":
        return self._skt.getpeername()

    def _recv_all(self, n: int) -> bytes:
        data = bytearray()

        while len(data) < n:
            read = self._skt.recv(n - len(data), socket.MSG_WAITALL)
            if not read:
                raise OSError("couldn't recv whole message")

            data.extend(read)

        return bytes(data)

    def recv(self) -> Bet:
        bet_size_bytes = self._recv_all(BET_SIZE_SIZE)
        bet_size = int.from_bytes(bet_size_bytes, "big")

        bet_bytes = self._recv_all(bet_size)
        return Bet.from_bytes(bet_bytes)

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
        return BetSockStream(skt), addr

    def close(self):
        self._skt.close()
