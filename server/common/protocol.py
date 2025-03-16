import socket

MSG_SIZE_SIZE = 4
DELIMITER = "\n"
TERMINATOR = ";"

"""
                                    MESSAGE PROTOCOL

The protocol has 7 fields, the first field is the size of the entire message in 4 bytes, after
that come the agency, name, surname, id, birthdate and number. All separated by a new line character.

"""


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

    def encode(self) -> bytes:
        atts = (
            self._agency,
            self._name,
            self._surname,
            self._id,
            self._birthdate,
            self._number,
        )

        return DELIMITER.join(atts).encode() + TERMINATOR.encode()


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

    def send(self, msg: Message):
        self._skt.sendall(msg.encode())

    def _recv_all(self) -> bytes:
        data = bytearray()

        while True:
            read = self._skt.recv(1024)
            if not read:
                raise OSError("inner socket got unexpectedly closed")

            data += read
            if read[-1] == TERMINATOR:
                return bytes(data)

    def recv(self) -> Message:
        return Message.from_bytes(self._recv_all())

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
