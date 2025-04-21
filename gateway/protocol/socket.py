import socket
from typing import Self, Any

# Files
FILE_MOVIES  = 0
FILE_CREDITS = 1
FILE_RATINGS = 2

# MSG kinds
MSG_BATCH = 0
MSG_FIN   = 1
MSG_ERR   = 2

def _recv_all(skt: socket.socket, n: int) -> bytearray:
    data = bytearray()
    while len(data) < n:
        read = skt.recv(n - len(data))
        if not read:
            raise ValueError("couldn't recv data through the socket")
        data.extend(read)
    return data

class Message:
    def __init__(self, kind: int, data: Any):
        self.kind = kind
        self.data = data

class CsvTransferStream:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    def resource(self):
        file_id_bytes = _recv_all(self._skt, 1)
        file_id = int.from_bytes(file_id_bytes, "big")
        return ["movies", "credits", "ratings"][file_id]

    def recv(self) -> Message:
        msg_kind_bytes = _recv_all(self._skt, 1)
        msg_kind = int.from_bytes(msg_kind_bytes, "big")
        if msg_kind != MSG_BATCH:
            return Message(msg_kind, None)

        batch_size_bytes = _recv_all(self._skt, 4)
        batch_size = int.from_bytes(batch_size_bytes, "big")

        lines = []
        for _ in range(batch_size):
            len_bytes = _recv_all(self._skt, 4)
            len = int.from_bytes(len_bytes, "big")
            line = _recv_all(self._skt, len)
            lines.append(line)

        return Message(msg_kind, lines)

    def send(self, data: bytearray):
        self._skt.sendall(data)

    def close(self):
        self._skt.close()

class CsvTransferListener:
    def __init__(self, skt: socket.socket):
        self._skt = skt

    @classmethod
    def bind(cls, host: str, port: int, backlog: int = 0) -> Self:
        """
        Instanciates a new CsvTransferListener and binds it to the given address
        """
        self = cls(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._skt.bind((host, port))
        self._skt.listen(backlog)
        return self

    def accept(self) -> tuple[CsvTransferStream, "socket._RetAddress"]:
        """
        Blocks the calling thread until a new connection arrives
        """
        skt, addr = self._skt.accept()
        return CsvTransferStream(skt), addr

    def shutdown(self, how: int):
        self._skt.shutdown(how)

    def close(self):
        self._skt.close()
