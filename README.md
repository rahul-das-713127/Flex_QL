# FlexQL (C++17)

This repository contains a SQL-like database server (FlexQL) implemented in C++17. It provides:

- A TCP server on port `9000`
- A client library (`flexql.h` / `flexql_client.cpp`)
- A REPL client (`client_repl.cpp` → `./client`)
- A benchmark + unit-test runner (`benchmark_flexql.cpp` → `./benchmark`)

## Directory structure

```text
flexql/
  Makefile
  server_full.cpp        # Full server implementation (recommended)
  server.cpp             # Simple/reference implementation (separate)
  flexql.h               # Client C API
  flexql_client.cpp      # Client library
  client_repl.cpp        # Interactive REPL client
  benchmark_flexql.cpp   # Benchmark + unit tests
  server                 # built binary
  client                 # built binary
  benchmark              # built binary
  data/
    catalog.txt          # schema catalog (created by server_full)
    wal.log              # WAL (created when enabled)
    *.data               # per-table table data files
```

## Dependencies

### Required

- `g++` (or `clang++`) with C++17 support
- `make`
- `pthread` (linked via `-lpthread`)

### Optional (useful for troubleshooting)

- `ss` (usually in `iproute2`)
- `lsof`

## Compilation

From the project root (`flexql/`):

```bash
make all
```

Targets:

- `make server`
- `make client`
- `make benchmark`

## Running the server

### Full server (recommended)

In one terminal:

```bash
./server
```

The server listens on `127.0.0.1:9000`.

### Environment variables

- `FLEXQL_PRESERVE_DATA=1`
  - Preserve `data/` across restarts.
  - If not set, the server wipes `data/` on startup.
- `FLEXQL_ENABLE_WAL=1`
  - Enable write-ahead logging (`data/wal.log`).
- `FLEXQL_DISABLE_WAL=1`
  - Force WAL disabled.

Example (preserve data + enable WAL):

```bash
FLEXQL_PRESERVE_DATA=1 FLEXQL_ENABLE_WAL=1 ./server
```

## Running the REPL client

In another terminal:

```bash
./client
```

Example session:

```sql
CREATE TABLE USERS(ID INT, NAME VARCHAR(64), EXPIRES_AT DATETIME);
INSERT INTO USERS VALUES (1, 'Alice', 1893456000);
SELECT * FROM USERS;
.exit
```

## Running the benchmark

### Unit tests only

```bash
./benchmark --unit-test
```

### Insert benchmark (N rows)

```bash
./benchmark 10000000
```

Notes:

- The benchmark connects to `127.0.0.1:9000`.
- It prints progress and reports throughput (rows/sec).

## Storage model (high level)

- Persistent state lives under `data/`.
- Table rows are appended to per-table `*.data` files.
- `catalog.txt` stores schemas.
- If WAL is enabled, SQL statements are appended to `wal.log` and replayed on startup.

## Troubleshooting

### `bind failed` / port 9000 already in use

This means an older server process is still listening.

#### Find the PID using `ss`

```bash
ss -ltnp | grep ":9000"
```

Then stop it:

```bash
kill <PID>
# if needed:
kill -9 <PID>
```

#### Find the PID using `lsof`

```bash
lsof -iTCP:9000 -sTCP:LISTEN -n -P
kill <PID>
```

### Reset the database

If you want a clean database state:

- Stop the server
- Delete the `data/` directory contents (or just unset `FLEXQL_PRESERVE_DATA` and restart)

## Documentation

- Detailed design: `design.tex`
- Performance results: `results.md`
