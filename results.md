# FlexQL Performance Results (Large Datasets)

This document records benchmark results for large insert workloads using the provided `./benchmark` client.

## Environment

- **Server**: `./server` (built from `server_full.cpp`)
- **Client**: `./benchmark`
- **Host OS**: Linux
- **Port**: `127.0.0.1:9000`

### Storage and durability configuration

The server can run in two main modes:

- **Fresh DB each start (default)**: server wipes `data/` on startup.
- **Persistent DB across restarts**: start server with `FLEXQL_PRESERVE_DATA=1`.

WAL:

- **Disabled by default**.
- Enable with `FLEXQL_ENABLE_WAL=1`.

## Results

### 10,000,000 row insert benchmark

Command:

```bash
./benchmark 10000000
```

Observed output:

- **Rows inserted**: `10,000,000`
- **Elapsed**: `9721 ms`
- **Throughput**: `1,028,700 rows/sec`

Notes:

- This run exercises the optimized insert path(s) in the server.
- If WAL is enabled, throughput will depend on disk performance and WAL flush/batching behavior.

### WAL-enabled behavior notes

When WAL is enabled (`FLEXQL_ENABLE_WAL=1`), the server writes SQL records to `data/wal.log`:

- Each record stores: `len` + SQL bytes + `crc`.
- WAL is replayed on startup to rebuild state.

Operational caveat:

- If `data/wal.log` is deleted while the server is running, the server may continue writing to an unlinked file handle (file not visible in directory listings). To measure WAL size, delete/rotate WAL only while the server is stopped.

## How to reproduce

1. Build:

```bash
make all
```

2. Start server in another terminal:

```bash
./server
```

3. Run benchmark:

```bash
./benchmark 10000000
```

## Interpreting results

- The benchmark measures end-to-end insert execution time as observed by the client.
- Server performance depends on:
  - write buffering thresholds
  - disk bandwidth/latency
  - WAL enabled/disabled
  - OS page cache behavior
  - concurrent load
