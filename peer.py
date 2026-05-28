import argparse
import importlib
import socket
import sys
import threading
import time

from Protocol import (
    MSG_BITFIELD, MSG_BLOCK, MSG_ERROR, MSG_HAVE, MSG_REQUEST,
    build_bitfield, build_block, build_error, build_have, build_request,
    recv_message,
)
from metadata import load_metadata, verify_file

_bm_mod = importlib.import_module("Block manager")
BlockManager = _bm_mod.BlockManager


class PeerConnection:
    def __init__(self, sock: socket.socket, addr: tuple, peer: "Peer"):
        self.sock = sock
        self.addr = addr
        self.peer = peer
        self.remote_map: list[bool] = [False] * peer.bm.total_blocks
        self._send_lock = threading.Lock()
        self._alive = True

    def send(self, data: bytes) -> bool:
        if not self._alive:
            return False
        with self._send_lock:
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                self._alive = False
                return False

    def run(self):
        self.send(build_bitfield(self.peer.bm.block_map))

        while self._alive:
            msg = recv_message(self.sock)
            if msg is None:
                break
            header, data = msg
            self.peer._handle(self, header, data)

        self._alive = False
        self.peer._remove_conn(self)
        try:
            self.sock.close()
        except OSError:
            pass
        print(f"[CONN] Conexão encerrada com {self.addr[0]}:{self.addr[1]}")


class Peer:
    def __init__(
        self,
        host: str,
        port: int,
        metadata: dict,
        storage_dir: str,
        neighbors: list[tuple[str, int]],
        source_file: str | None = None,
    ):
        self.host = host
        self.port = port
        self.bm = BlockManager(metadata, storage_dir)
        self.neighbors = neighbors
        self._is_seeder = source_file is not None

        self._conns: list[PeerConnection] = []
        self._conns_lock = threading.Lock()
        # assembly roda na main thread; _all_received sinaliza que os blocos chegaram
        self._all_received = threading.Event()
        self._done = threading.Event()

        if source_file:
            self.bm.mark_all_owned(source_file)
            print(f"[PEER] Modo Seeder — {self.bm.total_blocks} blocos disponíveis.")
        else:
            print(f"[PEER] Modo Leecher — {self.bm.total_blocks} blocos para baixar.")

    def start(self):
        threading.Thread(target=self._server_loop, daemon=True, name="server").start()
        time.sleep(0.1)

        for addr in self.neighbors:
            threading.Thread(target=self._connect_to, args=(addr,), daemon=True).start()

        if not self._is_seeder:
            threading.Thread(target=self._download_loop, daemon=True, name="downloader").start()

    def wait(self):
        try:
            if self._is_seeder:
                print("[PEER] Seeder ativo. Pressione Ctrl-C para encerrar.")
                threading.Event().wait()
            else:
                self._all_received.wait()
                self._assemble()
        except KeyboardInterrupt:
            print("\n[PEER] Encerrado pelo usuário.")

    def _server_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(20)
        print(f"[PEER] Servidor ouvindo em {self.host}:{self.port}")
        while True:
            try:
                conn, addr = srv.accept()
                print(f"[PEER] Conexão recebida de {addr[0]}:{addr[1]}")
                pc = PeerConnection(conn, addr, self)
                self._add_conn(pc)
                threading.Thread(target=pc.run, daemon=True).start()
            except OSError:
                break

    def _connect_to(self, addr: tuple[str, int]):
        host, port = addr
        for attempt in range(10):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((host, port))
                sock.settimeout(None)
                print(f"[PEER] Conectado a {host}:{port}")
                pc = PeerConnection(sock, (host, port), self)
                self._add_conn(pc)
                pc.run()
                return
            except (ConnectionRefusedError, TimeoutError, OSError):
                wait = 1 + attempt * 0.5
                print(f"[PEER] Tentativa {attempt+1}/10 — {host}:{port} indisponível, aguardando {wait:.1f}s")
                time.sleep(wait)
        print(f"[PEER] Não foi possível conectar a {host}:{port} após 10 tentativas.")

    def _add_conn(self, pc: PeerConnection):
        with self._conns_lock:
            self._conns.append(pc)

    def _remove_conn(self, pc: PeerConnection):
        with self._conns_lock:
            try:
                self._conns.remove(pc)
            except ValueError:
                pass

    def _broadcast(self, data: bytes, exclude: PeerConnection | None = None):
        with self._conns_lock:
            targets = [c for c in self._conns if c is not exclude and c._alive]
        for conn in targets:
            conn.send(data)

    def _handle(self, conn: PeerConnection, header: dict, data: bytes):
        msg_type = header.get("type")

        if msg_type == MSG_BITFIELD:
            bits = header.get("bitfield", "")
            conn.remote_map = [b == "1" for b in bits]
            n = sum(conn.remote_map)
            print(f"[PROTO] BITFIELD de {conn.addr[0]}:{conn.addr[1]} — {n}/{self.bm.total_blocks} blocos")

        elif msg_type == MSG_HAVE:
            block_id = header["block_id"]
            if 0 <= block_id < self.bm.total_blocks:
                conn.remote_map[block_id] = True

        elif msg_type == MSG_REQUEST:
            block_id = header["block_id"]
            blk = self.bm.read_block(block_id)
            if blk is not None:
                conn.send(build_block(block_id, blk))
            else:
                conn.send(build_error(block_id))

        elif msg_type == MSG_BLOCK:
            block_id = header["block_id"]
            if not self.bm.has_block(block_id):
                if self.bm.write_block(block_id, data):
                    self._broadcast(build_have(block_id))
                    owned = self.bm.owned_count()
                    total = self.bm.total_blocks
                    print(f"[PEER] Progresso: {owned}/{total} blocos ({100*owned//total}%)")
                    if self.bm.is_complete():
                        self._all_received.set()

        elif msg_type == MSG_ERROR:
            block_id = header.get("block_id", "?")
            print(f"[PROTO] ERRO: bloco {block_id} não disponível em {conn.addr[0]}:{conn.addr[1]}")

    def _download_loop(self):
        in_flight: set[int] = set()
        iteration = 0

        while not self.bm.is_complete():
            iteration += 1
            if iteration % 100 == 0:
                in_flight.clear()

            in_flight = {b for b in in_flight if not self.bm.has_block(b)}
            to_request = set(self.bm.missing_blocks()) - in_flight

            with self._conns_lock:
                conns = [c for c in self._conns if c._alive]

            if not conns:
                time.sleep(0.2)
                continue

            sent_any = False
            for block_id in sorted(to_request):
                for conn in conns:
                    if conn.remote_map[block_id]:
                        if conn.send(build_request(block_id)):
                            in_flight.add(block_id)
                            sent_any = True
                        break

            time.sleep(0.05 if sent_any else 0.2)

        self._all_received.set()

    def _assemble(self):
        print(f"\n[PEER] Todos os {self.bm.total_blocks} blocos recebidos! Remontando arquivo...")
        output = self.bm.assemble()
        ok = verify_file(output, self.bm.sha256)
        if ok:
            self.bm.cleanup_blocks()
            print(f"[PEER] ✓ Download concluído com sucesso!")
        else:
            print(f"[PEER] ✗ FALHA na verificação de integridade!")
        print(f"[PEER] Arquivo salvo em: {output}")
        self._done.set()


def parse_args():
    p = argparse.ArgumentParser(description="Peer P2P — Transferência de Arquivos")
    p.add_argument("--host",      default="127.0.0.1")
    p.add_argument("--port",      type=int, required=True)
    p.add_argument("--meta",      required=True)
    p.add_argument("--storage",   required=True)
    p.add_argument("--neighbors", nargs="*", default=[])
    p.add_argument("--file",      default=None)
    return p.parse_args()


def main():
    args = parse_args()

    neighbors = []
    for nb in (args.neighbors or []):
        host, port_str = nb.rsplit(":", 1)
        neighbors.append((host, int(port_str)))

    metadata = load_metadata(args.meta)

    peer = Peer(
        host=args.host,
        port=args.port,
        metadata=metadata,
        storage_dir=args.storage,
        neighbors=neighbors,
        source_file=args.file,
    )
    peer.start()
    peer.wait()


if __name__ == "__main__":
    main()
