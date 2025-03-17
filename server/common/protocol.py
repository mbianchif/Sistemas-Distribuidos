import socket

DELIMITER = ","
TERMINATOR = ";"
BATCH_SIZE_SIZE = 4
BATCH_COUNT_SIZE = 4


class MsgBet:
    def __init__(
        self, agency: str, name: str, surname: str, id: str, birthdate: str, number: str
    ):
        self._agency = agency
        self._name = name
        self._surname = surname
        self._id = id
        self._birthdate = birthdate
        self._number = number

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(*data.decode().split(DELIMITER))


class BetSockStream:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def connect(cls, host: str, port: int):
        """
        Instanciates a new BetSockStream connected to the given address
        """
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        skt.connect((host, port))
        return cls(skt)

    def peer_addr(self) -> "socket._RetAddress":
        return self._skt.getpeername()

    def _recv_n(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            read = self._skt.recv(n - len(data))
            if not read:
                raise OSError("connection closed unexpectedly")
            data.extend(read)

        return bytes(data)

    def recv(self) -> list[MsgBet]:
        nbatches_bytes = self._recv_n(BATCH_COUNT_SIZE)
        nbatches = int.from_bytes(nbatches_bytes, "big")
        bets = []

        for _ in range(nbatches):
            batch_size_bytes = self._recv_n(BATCH_SIZE_SIZE)
            batch_size = int.from_bytes(batch_size_bytes, "big")

            batch_bytes = self._recv_n(batch_size)
            for bet_bytes in batch_bytes.split(TERMINATOR.encode()):
                bet = MsgBet.from_bytes(bet_bytes)
                bets.append(bet)

        return bets

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
