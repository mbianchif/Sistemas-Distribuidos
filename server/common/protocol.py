import socket
from typing import Self

MSG_SIZE_SIZE = 4

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
    def from_bytes(cls, data: bytes) -> Self:
        return cls(*data.decode().split("\n"))

    def encode(self) -> bytes:
        atts = (
            self._agency,
            self._name,
            self._surname,
            self._id,
            self._birthdate,
            self._number,
        )

        return "\n".join(atts).encode()


class BetSockStream:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def connect(cls, host: str, port: int) -> Self:
        """
        Instanciates a new BetSockStream connected to the given address
        """
        self = cls(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._skt.connect((host, port))
        return self

    def peer_addr(self) -> socket._RetAddress:
        return self._skt.getpeername()

    def send(self, msg: Message):
        data = msg.encode()
        self._skt.sendall(len(data).to_bytes(MSG_SIZE_SIZE))
        self._skt.sendall(data)

    def _recv_all(self, n: int) -> bytes:
        buff = bytearray()
        while len(buff) < n:
            read = self._skt.recv(n - len(buff))
            if not read:
                raise OSError("peer socket is closed")

            buff.extend(read)

        return bytes(buff)

    def recv(self) -> Message:
        size = int.from_bytes(self._recv_all(MSG_SIZE_SIZE))
        data = self._recv_all(size)
        return Message.from_bytes(data)

    def close(self):
        self._skt.close()


class BetSockListener:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def bind(cls, host: str, port: int, backlog: int = 0) -> Self:
        """
        Instanciates a new BetSockListener and binds it to the given address
        """
        self = cls(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._skt.bind((host, port))
        self._skt.listen(backlog)
        return self

    def accept(self) -> tuple[BetSockStream, socket._RetAddress]:
        """
        Blocks the calling thread until a new connection arrives
        """
        skt, addr = self._skt.accept()
        return BetSockStream(skt), addr

    def close(self):
        self._skt.close()
