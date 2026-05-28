import json
import struct
import socket

MSG_REQUEST  = "REQUEST"
MSG_BLOCK    = "BLOCK"
MSG_HAVE     = "HAVE"
MSG_BITFIELD = "BITFIELD"
MSG_ERROR    = "ERROR"

# formato do frame: [header_len: 4B][data_len: 4B][header JSON][data binário]
_HEADER_FMT  = "<II"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


def build_request(block_id: int) -> bytes:
    return _pack({"type": MSG_REQUEST, "block_id": block_id}, b"")


def build_block(block_id: int, data: bytes) -> bytes:
    return _pack({"type": MSG_BLOCK, "block_id": block_id, "data_len": len(data)}, data)


def build_have(block_id: int) -> bytes:
    return _pack({"type": MSG_HAVE, "block_id": block_id}, b"")


def build_bitfield(block_map: list[bool]) -> bytes:
    bits = "".join("1" if b else "0" for b in block_map)
    return _pack({"type": MSG_BITFIELD, "bitfield": bits}, b"")


def build_error(block_id: int, reason: str = "not available") -> bytes:
    return _pack({"type": MSG_ERROR, "block_id": block_id, "reason": reason}, b"")


def _pack(header: dict, data: bytes) -> bytes:
    header_bytes = json.dumps(header).encode("utf-8")
    prefix = struct.pack(_HEADER_FMT, len(header_bytes), len(data))
    return prefix + header_bytes + data


def recv_message(sock: socket.socket) -> tuple[dict, bytes] | None:
    """Lê uma mensagem completa do socket. Retorna None se a conexão foi encerrada."""
    prefix = _recv_exact(sock, _HEADER_SIZE)
    if prefix is None:
        return None

    header_len, data_len = struct.unpack(_HEADER_FMT, prefix)
    raw = _recv_exact(sock, header_len + data_len)
    if raw is None:
        return None

    header = json.loads(raw[:header_len].decode("utf-8"))
    data   = raw[header_len:]
    return header, data


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except (ConnectionResetError, OSError):
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


if __name__ == "__main__":
    import threading

    def server(port, results):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", port))
        srv.listen(1)
        conn, _ = srv.accept()

        messages = []
        while True:
            msg = recv_message(conn)
            if msg is None:
                break
            messages.append(msg)

        results.extend(messages)
        conn.close()
        srv.close()

    PORT    = 15555
    results = []
    t = threading.Thread(target=server, args=(PORT, results), daemon=True)
    t.start()

    import time; time.sleep(0.1)

    cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cli.connect(("127.0.0.1", PORT))

    cli.sendall(build_request(3))
    cli.sendall(build_block(3, b"hello_world_data"))
    cli.sendall(build_have(3))
    cli.sendall(build_bitfield([True, False, True, True]))
    cli.sendall(build_error(7, "not available"))
    cli.close()

    t.join(timeout=2)

    assert len(results) == 5, f"Esperava 5 mensagens, recebeu {len(results)}"

    h, d = results[0]; assert h["type"] == MSG_REQUEST  and h["block_id"] == 3
    h, d = results[1]; assert h["type"] == MSG_BLOCK    and d == b"hello_world_data"
    h, d = results[2]; assert h["type"] == MSG_HAVE     and h["block_id"] == 3
    h, d = results[3]; assert h["type"] == MSG_BITFIELD and h["bitfield"] == "1011"
    h, d = results[4]; assert h["type"] == MSG_ERROR    and h["block_id"] == 7

    print("✓ Todos os 5 tipos de mensagem serializados e desserializados corretamente.")
