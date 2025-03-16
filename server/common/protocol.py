import socket

BATCH_SIZE_SIZE = 4
DELIMITER = ","
TERMINATOR = ";"


class Message:
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

    def recv(self) -> list[Message]:
        def read_n(n: int) -> bytes:
            data = bytearray()
            while len(data) < n:
                read = self._skt.recv(n - len(data))
                if not read:
                    raise OSError("connection closed unexpectedly")
                data += read
            return bytes(data)

        batch_size_bytes = read_n(BATCH_SIZE_SIZE)
        batch_size = int.from_bytes(batch_size_bytes, "big")
        batch = read_n(batch_size)
        terminator_byte = TERMINATOR.encode()
        return [Message.from_bytes(chunk) for chunk in batch.split(terminator_byte)]

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
