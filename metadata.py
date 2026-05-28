import json
import hashlib
import math
import os

BLOCK_SIZE_DEFAULT = 1024


def compute_sha256(filepath: str) -> str:
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            sha.update(chunk)
    return sha.hexdigest()


def create_metadata(filepath: str, block_size: int = BLOCK_SIZE_DEFAULT) -> dict:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    file_size = os.path.getsize(filepath)
    return {
        "filename": os.path.basename(filepath),
        "file_size": file_size,
        "block_size": block_size,
        "total_blocks": math.ceil(file_size / block_size),
        "sha256": compute_sha256(filepath),
    }


def save_metadata(metadata: dict, output_path: str) -> None:
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"[META] Metadado salvo em: {output_path}")


def load_metadata(meta_path: str) -> dict:
    if not os.path.exists(meta_path):
        raise FileNotFoundError(f"Arquivo .meta não encontrado: {meta_path}")
    with open(meta_path, "r") as f:
        metadata = json.load(f)
    print(f"[META] Metadado carregado: {metadata['filename']} "
          f"({metadata['total_blocks']} blocos de {metadata['block_size']} bytes)")
    return metadata


def verify_file(filepath: str, expected_hash: str) -> bool:
    actual_hash = compute_sha256(filepath)
    ok = actual_hash == expected_hash
    if ok:
        print(f"[META] ✓ Integridade verificada: {filepath}")
    else:
        print(f"[META] ✗ FALHA na integridade!")
        print(f"       Esperado:  {expected_hash}")
        print(f"       Calculado: {actual_hash}")
    return ok


if __name__ == "__main__":
    import tempfile, random

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp.write(bytes(random.getrandbits(8) for _ in range(10 * 1024)))
        tmp_path = tmp.name

    print(f"Arquivo de teste criado: {tmp_path}")

    meta = create_metadata(tmp_path, block_size=1024)
    print(f"Metadados gerados: {json.dumps(meta, indent=2)}")

    meta_path = tmp_path + ".meta"
    save_metadata(meta, meta_path)

    loaded = load_metadata(meta_path)
    assert loaded == meta, "Metadados não batem após salvar/carregar!"
    print("✓ save/load OK")

    assert verify_file(tmp_path, meta["sha256"]), "Hash não bateu!"

    os.unlink(tmp_path)
    os.unlink(meta_path)
    print("✓ Todos os testes passaram.")
