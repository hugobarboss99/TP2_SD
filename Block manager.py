import os
import threading


class BlockManager:
    """Rastreia blocos locais, lê/escreve do disco e remonta o arquivo final."""

    def __init__(self, metadata: dict, storage_dir: str):
        self.filename     = metadata["filename"]
        self.file_size    = metadata["file_size"]
        self.block_size   = metadata["block_size"]
        self.total_blocks = metadata["total_blocks"]
        self.sha256       = metadata["sha256"]

        self.storage_dir  = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

        self.block_map: list[bool] = [False] * self.total_blocks
        self._lock = threading.Lock()
        self._output_path = os.path.join(storage_dir, self.filename)

        # controla criação única do arquivo pré-alocado
        self._file_ready = False
        self._file_lock  = threading.Lock()

    def mark_all_owned(self, source_filepath: str) -> None:
        self._source_path = source_filepath
        with self._lock:
            self.block_map = [True] * self.total_blocks
        print(f"[BM] Seeder pronto: {self.total_blocks} blocos disponíveis.")

    def read_block(self, block_id: int) -> bytes | None:
        with self._lock:
            if not self.block_map[block_id]:
                return None

        offset = block_id * self.block_size

        # Seeder lê do arquivo fonte; leecher lê do arquivo de saída já montado
        path = getattr(self, "_source_path", self._output_path)
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(self.block_size)

    def write_block(self, block_id: int, data: bytes) -> bool:
        with self._lock:
            if self.block_map[block_id]:
                return True

        self._ensure_output_file()

        # escreve direto no offset correto — sem criar arquivos intermediários
        offset = block_id * self.block_size
        try:
            with open(self._output_path, "r+b") as f:
                f.seek(offset)
                f.write(data)
            with self._lock:
                self.block_map[block_id] = True
            print(f"[BM] Bloco {block_id:04d} salvo ({len(data)} bytes).")
            return True
        except IOError as e:
            print(f"[BM] ERRO ao salvar bloco {block_id}: {e}")
            return False

    def _ensure_output_file(self):
        if self._file_ready:
            return
        with self._file_lock:
            if self._file_ready:
                return
            with open(self._output_path, "wb") as f:
                f.seek(self.file_size - 1)
                f.write(b"\0")
            self._file_ready = True

    def has_block(self, block_id: int) -> bool:
        with self._lock:
            return self.block_map[block_id]

    def missing_blocks(self) -> list[int]:
        with self._lock:
            return [i for i, owned in enumerate(self.block_map) if not owned]

    def is_complete(self) -> bool:
        with self._lock:
            return all(self.block_map)

    def owned_count(self) -> int:
        with self._lock:
            return sum(self.block_map)

    def assemble(self) -> str:
        """Verifica integridade do arquivo final. Lança RuntimeError se incompleto."""
        if not self.is_complete():
            raise RuntimeError(f"Não é possível remontar: faltam {len(self.missing_blocks())} blocos.")

        if hasattr(self, "_source_path"):
            print(f"[BM] Seeder: arquivo original em {self._source_path}")
            return self._source_path

        actual_size = os.path.getsize(self._output_path)
        if actual_size != self.file_size:
            raise RuntimeError(
                f"Tamanho incorreto: esperado {self.file_size}, obtido {actual_size}"
            )

        print(f"[BM] ✓ Arquivo remontado: {self._output_path} ({actual_size} bytes)")
        return self._output_path

    def cleanup_blocks(self) -> None:
        pass  # blocos escritos direto no arquivo final; nada a remover

    def __repr__(self) -> str:
        return (
            f"<BlockManager '{self.filename}' "
            f"{self.owned_count()}/{self.total_blocks} blocos>"
        )


if __name__ == "__main__":
    import tempfile, random, shutil
    from metadata import create_metadata, verify_file

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        content = bytes(random.getrandbits(8) for _ in range(10 * 1024))
        tmp.write(content)
        src_path = tmp.name

    meta = create_metadata(src_path, block_size=1024)
    storage = tempfile.mkdtemp()

    seeder_bm = BlockManager(meta, storage + "/seeder")
    seeder_bm.mark_all_owned(src_path)
    assert seeder_bm.is_complete()
    print(f"Seeder: {seeder_bm}")

    leecher_bm = BlockManager(meta, storage + "/leecher")
    assert not leecher_bm.is_complete()
    print(f"Leecher antes: {leecher_bm}")

    for block_id in leecher_bm.missing_blocks():
        data = seeder_bm.read_block(block_id)
        assert data is not None, f"Seeder não tem bloco {block_id}"
        leecher_bm.write_block(block_id, data)

    assert leecher_bm.is_complete()
    print(f"Leecher depois: {leecher_bm}")

    output = leecher_bm.assemble()
    ok = verify_file(output, meta["sha256"])
    assert ok, "Hash não bateu após remontagem!"

    leecher_bm.cleanup_blocks()

    shutil.rmtree(storage)
    os.unlink(src_path)
    print("✓ Todos os testes do BlockManager passaram.")
