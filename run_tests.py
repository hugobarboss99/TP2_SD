#!/usr/bin/env python3
# Testes automáticos — cenários da Tabela 1 do enunciado (SD_TP_2.pdf)
# Uso: python run_tests.py [A|B|C|A1|B2...] [-v]

import hashlib
import os
import shutil
import subprocess
import sys
import threading
import time

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

from metadata import create_metadata, save_metadata

PYTHON      = sys.executable
PEER_SCRIPT = os.path.join(PROJECT_DIR, "peer.py")
BASE_PORT   = 6100


def make_random_file(path: str, size: int):
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            f.write(os.urandom(min(65536, remaining)))
            remaining -= min(65536, remaining)


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def monitor_output(proc: subprocess.Popen, label: str,
                   done_ev: threading.Event, verbose: bool):
    for line in proc.stdout:
        line = line.rstrip()
        if verbose:
            print(f"    [{label}] {line}")
        if "Download concluído" in line or "✓ Download" in line:
            done_ev.set()
    done_ev.set()


def run_scenario(
    name: str,
    file_size: int,
    block_size: int,
    num_peers: int,
    timeout: int,
    port_base: int,
    verbose: bool = False,
) -> dict:
    workdir = os.path.join("/tmp", f"p2p_{name.replace(' ', '_').replace('/', '_')}")
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir)

    result = {
        "name": name, "file_size": file_size, "block_size": block_size,
        "num_peers": num_peers, "ok": False, "elapsed": 0.0, "error": "",
    }

    processes: list[subprocess.Popen] = []

    try:
        src       = os.path.join(workdir, "source.bin")
        meta_path = os.path.join(workdir, "source.meta")
        make_random_file(src, file_size)
        original_hash = sha256(src)
        meta = create_metadata(src, block_size=block_size)
        save_metadata(meta, meta_path)
        filename = meta["filename"]

        seeder_port   = port_base
        leecher_ports = [port_base + i + 1 for i in range(num_peers - 1)]

        seeder_dir = os.path.join(workdir, "peer_0")
        os.makedirs(seeder_dir)
        seeder_proc = subprocess.Popen(
            [PYTHON, PEER_SCRIPT,
             "--port",    str(seeder_port),
             "--meta",    meta_path,
             "--storage", seeder_dir,
             "--file",    src],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=PROJECT_DIR,
        )
        processes.append(seeder_proc)
        threading.Thread(
            target=monitor_output,
            args=(seeder_proc, f"S:{seeder_port}", threading.Event(), verbose),
            daemon=True,
        ).start()
        time.sleep(0.4)

        leecher_info: list[tuple[int, subprocess.Popen, str, threading.Event]] = []

        for i, lport in enumerate(leecher_ports):
            ldir = os.path.join(workdir, f"peer_{i+1}")
            os.makedirs(ldir)

            neighbors = [f"127.0.0.1:{seeder_port}"]
            for prev in leecher_ports[:i]:
                neighbors.append(f"127.0.0.1:{prev}")

            proc = subprocess.Popen(
                [PYTHON, PEER_SCRIPT,
                 "--port",      str(lport),
                 "--meta",      meta_path,
                 "--storage",   ldir,
                 "--neighbors", *neighbors],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=PROJECT_DIR,
            )
            done_ev = threading.Event()
            processes.append(proc)
            leecher_info.append((i + 1, proc, ldir, done_ev))

            threading.Thread(
                target=monitor_output,
                args=(proc, f"L{i+1}:{lport}", done_ev, verbose),
                daemon=True,
            ).start()
            time.sleep(0.15)

        all_done = threading.Event()

        def wait_all():
            for _, _, _, ev in leecher_info:
                ev.wait()
            all_done.set()

        threading.Thread(target=wait_all, daemon=True).start()

        start = time.time()
        if not all_done.wait(timeout=timeout):
            result["error"] = f"TIMEOUT ({timeout}s)"
            return result

        result["elapsed"] = time.time() - start

        all_ok = True
        for idx, _, ldir, _ in leecher_info:
            out_file = os.path.join(ldir, filename)
            if not os.path.exists(out_file):
                result["error"] = f"Leecher{idx}: arquivo não encontrado"
                all_ok = False
                break
            if sha256(out_file) != original_hash:
                result["error"] = f"Leecher{idx}: hash SHA-256 incorreto"
                all_ok = False
                break

        result["ok"] = all_ok

    except Exception as exc:
        result["error"] = str(exc)

    finally:
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        shutil.rmtree(workdir, ignore_errors=True)

    return result


KB = 1024
MB = 1024 * 1024

SCENARIOS = [
    ("A1", "10 KB  | bloco 1 KB | 2 peers",  10*KB,   1*KB,  2,  30),
    ("A2", "20 KB  | bloco 1 KB | 2 peers",  20*KB,   1*KB,  2,  30),
    ("A3", "10 KB  | bloco 4 KB | 2 peers",  10*KB,   4*KB,  2,  30),
    ("A4", "10 KB  | bloco 1 KB | 4 peers",  10*KB,   1*KB,  4,  60),
    ("B1",  "1 MB  | bloco 1 KB | 2 peers",   1*MB,   1*KB,  2,  60),
    ("B2",  "5 MB  | bloco 1 KB | 2 peers",   5*MB,   1*KB,  2, 120),
    ("B3",  "1 MB  | bloco 4 KB | 2 peers",   1*MB,   4*KB,  2,  60),
    ("B4",  "1 MB  | bloco 1 KB | 4 peers",   1*MB,   1*KB,  4, 120),
    ("C1", "10 MB  | bloco 1 KB | 2 peers",  10*MB,   1*KB,  2, 180),
    ("C2", "20 MB  | bloco 1 KB | 2 peers",  20*MB,   1*KB,  2, 300),
    ("C3", "10 MB  | bloco 4 KB | 2 peers",  10*MB,   4*KB,  2, 180),
    ("C4", "10 MB  | bloco 1 KB | 4 peers",  10*MB,   1*KB,  4, 600),
]


def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    filter_args = [a.upper() for a in sys.argv[1:] if not a.startswith("-")]

    if filter_args:
        selected = [s for s in SCENARIOS if any(s[0].upper().startswith(f) for f in filter_args)]
        if not selected:
            print(f"Nenhum cenário encontrado para: {filter_args}")
            print("Prefixos válidos: A, B, C, A1, A2, B1, B2, C1, C2, ...")
            sys.exit(1)
    else:
        selected = SCENARIOS

    print("=" * 70)
    print("  TESTES AUTOMÁTICOS — Sistema P2P (SD TP2)")
    print("=" * 70)
    print(f"  {len(selected)} cenário(s) a executar")
    if verbose:
        print("  Modo verbose ativado — logs completos dos peers visíveis")
    print()

    results = []
    for run_idx, (prefix, desc, fsize, bsize, npeers, tout) in enumerate(selected):
        name = f"{prefix} — {desc}"
        port_base = BASE_PORT + run_idx * 10

        print(f"[{run_idx+1}/{len(selected)}] {name}")
        print(f"  Arquivo: {fsize//KB} KB  |  Bloco: {bsize} B  |  Peers: {npeers}  |  Timeout: {tout}s")
        if verbose:
            print()

        r = run_scenario(name, fsize, bsize, npeers, tout, port_base, verbose)
        results.append(r)

        status = "✓ OK" if r["ok"] else f"✗ {r['error']}"
        print(f"  → {status}  |  Tempo: {r['elapsed']:.2f}s")
        print()

        # pausa maior após arquivos grandes para liberar recursos do sistema
        time.sleep(3.0 if fsize >= 10*MB else 1.5)

    sep = "-" * 68
    print("=" * 68)
    print("  SUMÁRIO FINAL")
    print("=" * 68)
    print(f"  {'Cenário':<42} {'Status':<15} {'Tempo':>7}")
    print(f"  {sep}")

    passed = 0
    for r in results:
        status = "✓ PASSOU" if r["ok"] else f"✗ {r['error'][:14]}"
        print(f"  {r['name']:<42} {status:<15} {r['elapsed']:>6.2f}s")
        if r["ok"]:
            passed += 1

    print(f"  {sep}")
    print(f"  Resultado: {passed}/{len(results)} cenários passaram")
    print("=" * 68)

    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
