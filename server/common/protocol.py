import socket

BATCH_SIZE_SIZE = 4
MSG_SIZE_SIZE = 4
DELIMITER = "\n"
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

    def send(self, *msgs: Message):
        batch_size = len(msgs)
        data = bytearray(batch_size.to_bytes(BATCH_SIZE_SIZE, "big"))

        for msg in msgs:
            data.extend(msg.encode())

        self._skt.sendall(data)

    def recv(self) -> list[Message]:
        terminator_byte = TERMINATOR.encode()

        def recv_msg(bufsize: int):
            data = bytearray()
            while not data.endswith(terminator_byte):
                read = self._skt.recv(bufsize)
                if not read:
                    raise OSError("inner socket got unexpectedly closed")

                data += read

            return Message.from_bytes(bytes(data))

        batch_size = int.from_bytes(self._skt.recv(BATCH_SIZE_SIZE), "big")
        return [recv_msg(1024) for _ in range(batch_size)]

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
