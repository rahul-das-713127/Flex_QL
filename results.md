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

### Batch-size tuning (multi-row INSERT)

The benchmark client can batch multiple rows into a single statement:

```sql
INSERT INTO BIG_USERS VALUES (...),(...),(...);
```

This is controlled by the environment variable `FLEXQL_INSERT_BATCH_SIZE`.

On a 200,000-row run (WAL off), the following results were observed:

- **Batch 1**
  - Elapsed: `229 ms`
  - Throughput: `873,362 rows/sec`
- **Batch 5**
  - Elapsed: `198 ms`
  - Throughput: `1,010,101 rows/sec`
- **Batch 10**
  - Elapsed: `203 ms`
  - Throughput: `985,221 rows/sec`
- **Batch 20**
  - Elapsed: `191 ms`
  - Throughput: `1,047,120 rows/sec`
- **Batch 50**
  - Elapsed: `184 ms`
  - Throughput: `1,086,956 rows/sec`
- **Batch 100**
  - Elapsed: `173 ms`
  - Throughput: `1,156,069 rows/sec`
- **Batch 200**
  - Elapsed: `166 ms`
  - Throughput: `1,204,819 rows/sec`
- **Batch 500**
  - Elapsed: `181 ms`
  - Throughput: `1,104,972 rows/sec`
- **Batch 1000**
  - Elapsed: `180 ms`
  - Throughput: `1,111,111 rows/sec`

In this run, **batch size 200** was best.

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
