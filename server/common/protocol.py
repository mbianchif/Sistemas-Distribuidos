import socket

MSG_SIZE_SIZE = 4
DELIMITER = "\n"
TERMINATOR = ";"

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
        return cls(*data.decode().rstrip(TERMINATOR).split(DELIMITER))

    def encode(self) -> bytes:
        atts = (
            self._agency,
            self._name,
            self._surname,
            self._id,
            self._birthdate,
            self._number,
        )

        return (DELIMITER.join(atts) + TERMINATOR).encode()


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

    def _recv_all(self) -> bytes:
        data = bytearray()
        terminator_byte = TERMINATOR.encode()

        while True:
            read = self._skt.recv(1024)
            if not read:
                raise OSError("inner socket got unexpectedly closed")

            data += read
            if read.endswith(terminator_byte):
                return bytes(data)

    def recv(self) -> MsgBet:
        return MsgBet.from_bytes(self._recv_all())

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
