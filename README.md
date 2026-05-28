# Transferência de Arquivos Peer-to-Peer

Trabalho Prático 2 — Sistemas Distribuídos | CEFET-MG 2025/2

Sistema de transferência de arquivos P2P onde cada peer atua simultaneamente como servidor e cliente. O arquivo é fragmentado em blocos e transferido diretamente entre os nós da rede.

---

## Como funciona

O peer que possui o arquivo completo (**Seeder**) fragmenta-o em blocos de tamanho fixo. Os peers que precisam do arquivo (**Leechers**) conectam-se aos vizinhos configurados, trocam um mapa de blocos (BITFIELD) e solicitam os blocos que ainda não possuem. À medida que recebem blocos, eles também passam a servi-los para outros peers.

Protocolo de mensagens:

| Tipo      | Descrição                                    |
|-----------|----------------------------------------------|
| REQUEST   | Solicita um bloco específico ao peer         |
| BLOCK     | Responde com os dados do bloco solicitado    |
| HAVE      | Anuncia que passou a possuir um novo bloco   |
| BITFIELD  | Envia o mapa completo de blocos ao conectar  |
| ERROR     | Informa que o bloco solicitado não está disponível |

---

## Estrutura do projeto

```
Protocol.py       # serialização/desserialização das mensagens
metadata.py       # criação e verificação do arquivo .meta (SHA-256)
Block manager.py  # gerenciamento de blocos em disco
peer.py           # nó P2P (servidor + cliente simultâneos)
run_tests.py      # testes automáticos dos cenários do enunciado
```

---

## Requisitos

- Python 3.10+
- Sem dependências externas (só biblioteca padrão)

---

## Como rodar

### 1. Criar o arquivo de teste e o metadado

```bash
python3 -c "
import os
from metadata import create_metadata, save_metadata

os.makedirs('./storage', exist_ok=True)
with open('./storage/arquivo.bin', 'wb') as f:
    f.write(os.urandom(10 * 1024))

meta = create_metadata('./storage/arquivo.bin', block_size=1024)
save_metadata(meta, './storage/arquivo.meta')
"
```

### 2. Iniciar o Seeder (Terminal A)

```bash
python3 peer.py \
    --port 5001 \
    --meta ./storage/arquivo.meta \
    --storage ./storage/seeder \
    --file ./storage/arquivo.bin
```

### 3. Iniciar o Leecher (Terminal B)

```bash
python3 peer.py \
    --port 5002 \
    --meta ./storage/arquivo.meta \
    --storage ./storage/leecher \
    --neighbors 127.0.0.1:5001
```

O leecher encerra automaticamente ao terminar o download. O arquivo remontado fica em `./storage/leecher/arquivo.bin`.

### Com 4 peers

```bash
# Terminal A — Seeder
python3 peer.py --port 5001 --meta arquivo.meta --storage ./p0 --file arquivo.bin

# Terminal B — Leecher 1
python3 peer.py --port 5002 --meta arquivo.meta --storage ./p1 --neighbors 127.0.0.1:5001

# Terminal C — Leecher 2 (recebe do Seeder e do Leecher 1)
python3 peer.py --port 5003 --meta arquivo.meta --storage ./p2 --neighbors 127.0.0.1:5001 127.0.0.1:5002

# Terminal D — Leecher 3
python3 peer.py --port 5004 --meta arquivo.meta --storage ./p3 --neighbors 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003
```

---

## Testes automáticos

```bash
# Todos os 12 cenários
python3 run_tests.py

# Por grupo de arquivo
python3 run_tests.py A   # 10 KB / 20 KB
python3 run_tests.py B   # 1 MB / 5 MB
python3 run_tests.py C   # 10 MB / 20 MB

# Cenário específico com logs dos peers
python3 run_tests.py C4 --verbose
```

### Resultados (execução completa em sequência)

| Cenário                          | Status   | Tempo   |
|----------------------------------|----------|---------|
| A1 — 10 KB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 0.23s |
| A2 — 20 KB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 0.23s |
| A3 — 10 KB \| bloco 4 KB \| 2 peers | ✓ PASSOU | 0.23s |
| A4 — 10 KB \| bloco 1 KB \| 4 peers | ✓ PASSOU | 0.23s |
| B1 —  1 MB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 0.49s |
| B2 —  5 MB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 1.55s |
| B3 —  1 MB \| bloco 4 KB \| 2 peers | ✓ PASSOU | 0.24s |
| B4 —  1 MB \| bloco 1 KB \| 4 peers | ✓ PASSOU | 19.22s |
| C1 — 10 MB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 3.10s |
| C2 — 20 MB \| bloco 1 KB \| 2 peers | ✓ PASSOU | 6.36s |
| C3 — 10 MB \| bloco 4 KB \| 2 peers | ✓ PASSOU | 0.86s |
| C4 — 10 MB \| bloco 1 KB \| 4 peers | ✓ PASSOU | 3.25s |

> **Nota sobre B4:** o tempo elevado (19s) ocorre por acúmulo de sockets em estado `TIME_WAIT` após os cenários anteriores, o que faz os leechers falharem na primeira tentativa de conexão e aguardarem o backoff de reconexão. Executado isoladamente (`python3 run_tests.py B4`), completa em ~0.2s.

---

## Parâmetros do peer.py

| Parâmetro     | Descrição                                           |
|---------------|-----------------------------------------------------|
| `--host`      | IP deste peer (padrão: `127.0.0.1`)                |
| `--port`      | Porta TCP deste peer (obrigatório)                  |
| `--meta`      | Caminho para o arquivo `.meta`                      |
| `--storage`   | Diretório onde os blocos e o arquivo final são salvos |
| `--neighbors` | Lista de peers vizinhos no formato `host:port`      |
| `--file`      | Arquivo fonte completo — ativa o modo Seeder        |
