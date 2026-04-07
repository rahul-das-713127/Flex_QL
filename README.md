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

## Running the REPL client

In another terminal:

```bash
./client
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

