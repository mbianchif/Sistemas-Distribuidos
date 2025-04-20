BATCH = 0
EOF = 1
ERROR = 2

def lines_to_sanitize(lines: list[bytearray]) -> bytearray:
    acc = bytearray([BATCH])
    first = True

    for line in lines:
        if not first:
            acc.extend(b"\n")

        first = False
        acc.extend(line)

    return acc

def fin_to_sanitize(_: None) -> bytes:
    return EOF.to_bytes(1, "big")