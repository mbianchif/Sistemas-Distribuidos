from typing import List, Dict

# Field name to ID mapping
name2id = {
    # movies
    "id": 0,
    "title": 1,
    "release_date": 2,
    "overview": 3,
    "budget": 4,
    "revenue": 5,
    "genres": 6,
    "production_countries": 7,
    "spoken_languages": 8,

    # ratings
    "movieId": 9,
    "rating": 10,
    "timestamp": 11,

    # credits
    "cast": 12,

    # Added
    "rate_revenue_budget": 13,
    "sentiment": 14,
    "country": 15,
    "actor": 16,
    "count": 17,
}

# ID to field name mapping
id2name = [k for k, _ in sorted(name2id.items(), key=lambda x: x[1])]

# Message types
BATCH = 0
EOF = 1
ERROR = 2

_query_columns = {
    1: ["title", "genres"],
    2: ["country", "budget"],
    3: ["title", "rating"],
    4: ["actor", "count"],
    5: ["sentiment", "rate_revenue_budget"],
}

def read_delivery_with_query(body: bytes) -> tuple[int, int, bytes]:
    if len(body) < 2:
        return ERROR, -1, bytes()
    return body[0], body[1], body[2:]

class Batch:
    def __init__(self, field_maps: List[Dict[str, str]]):
        self.field_maps = field_maps

    @staticmethod
    def decode(data: bytes) -> "Batch":
        lines = data.splitlines()
        field_maps = [decode_line(line) for line in lines if line]
        return Batch(field_maps)

    def _encode_query_field_map(self, field_map: Dict[str, str], query: int) -> bytearray:
        must = _query_columns[query]
        line = bytearray()
        first = True

        for col in must:
            if col not in field_map:
                raise ValueError(f"no {col} field in query field_map for Q{query}")
            if not first:
                line.extend(b",")
            
            first = False
            value = field_map[col]

            if col == "genres":
                value = "[" + value + "]"
            
            line.extend(value.strip().encode())

        return line

    def to_result(self, query: int) -> bytearray:
        data = bytearray([0] * 4 + [BATCH, query])
        first = True

        for field_map in self.field_maps:
            if not first:
                data.extend(b"\n")

            first = False
            line_bytes = self._encode_query_field_map(field_map, query)
            data.extend(line_bytes)

        data_length = len(data) - 6
        data[:4] = data_length.to_bytes(4, "big")
        return data

def decode_line(data: bytes) -> Dict[str, str]:
    fields = {}
    for kv in data.split(b";"):
        if b"=" not in kv:
            continue

        key_id, val = kv.split(b"=", 1)
        try:
            key_num = int(key_id)
            key_name = id2name[key_num]
            fields[key_name] = val.decode("utf-8")
        except (ValueError, IndexError):
            continue

    return fields


class Eof:
    @staticmethod
    def decode(_: bytes) -> "Eof":
        return Eof()
    
    def to_result(self, query: int) -> bytearray:
        return bytearray([0] * 4 + [EOF, query])

class Error:
    @staticmethod
    def decode(_: bytes) -> "Error":
        return Error()

    def to_result(self, query: int) -> bytearray:
        return bytearray([0] * 4 + [ERROR, query])
