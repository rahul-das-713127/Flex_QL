#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#define PORT 9000
#define BUFFER_SIZE 65536

namespace flexql {

struct TableSchema;

static std::string upper(const std::string &s);
static std::string trim(const std::string &s);
static std::string_view trim_sv(std::string_view sv);
static bool starts_with_ci(std::string_view s, std::string_view prefix);
static inline bool is_ident_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

static bool validate_expires_at_value_sv(std::string_view sv, std::string &err);
static bool validate_expires_at_value(const std::string &s, std::string &err);

static std::optional<long double> to_number(const std::string &s);
static uint32_t crc32(const uint8_t *data, size_t len);
static std::vector<std::string> split_csv_top_level(const std::string &s);
static std::string table_data_path(const std::string &table_name);
static void ensure_runtime_for_table(const TableSchema &schema);
static bool validate_insert_row_values_sv(const TableSchema &schema, const std::vector<std::string_view> &values, std::string &err);
static bool validate_insert_row_values(const TableSchema &schema, const std::vector<std::string> &values, std::string &err);
static void register_file_with_async_writer(const std::string &tkey, FILE *fp);
static void flush_arena_to_async_writer(const std::string &tkey, std::string &arena, FILE *fp);
static bool fast_parse_insert_single_row_views_sv(std::string_view sql, std::string_view &out_table, std::vector<std::string_view> &out_vals, std::string &err);

static constexpr const char *DATA_DIR = "data";
static constexpr const char *WAL_PATH = "data/wal.log";
static constexpr const char *CATALOG_PATH = "data/catalog.txt";

enum class ColType {
    Int,
    Decimal,
    Varchar,
    Datetime,
    Unknown,
};

struct Column {
    std::string name;
    ColType type = ColType::Unknown;
    int varchar_len = -1;
};

struct TableSchema {
    std::string name;
    std::vector<Column> columns;
    std::unordered_map<std::string, size_t> col_index;

    std::optional<size_t> expires_at_col;
};

struct Condition {
    std::string lhs;
    std::string op;
    std::string rhs;
    bool rhs_is_string = false;
};

struct Database {
    std::unordered_map<std::string, TableSchema> schemas; // key: UPPER(table)
    struct TableRuntime {
        bool has_int_pk_index = false;
        size_t pk_col = 0;
        std::vector<uint64_t> pk_offsets;
        long long max_pk_seen = 0;
        FILE *data_fp = nullptr;
        uint64_t data_end_offset = 0;
        bool data_dirty = false;
        bool pk_index_valid = false;  // false = pk_offsets needs rebuild before use

        // Spinlock for fast uncontended single-client inserts.
        std::atomic_flag spin = ATOMIC_FLAG_INIT;

        // atomic_flag is not movable; provide explicit move ctor/assign
        // that default-initialises the spinlock in the destination.
        TableRuntime() = default;
        TableRuntime(TableRuntime &&o) noexcept
            : has_int_pk_index(o.has_int_pk_index),
              pk_col(o.pk_col),
              pk_offsets(std::move(o.pk_offsets)),
              max_pk_seen(o.max_pk_seen),
              data_fp(o.data_fp),
              data_end_offset(o.data_end_offset),
              data_dirty(o.data_dirty),
              pk_index_valid(o.pk_index_valid)
              // spin is default-initialised (unlocked) — safe because the
              // source is being destroyed and no thread can hold its lock.
        { o.data_fp = nullptr; }
        TableRuntime &operator=(TableRuntime &&o) noexcept {
            if (this != &o) {
                has_int_pk_index = o.has_int_pk_index;
                pk_col = o.pk_col;
                pk_offsets = std::move(o.pk_offsets);
                max_pk_seen = o.max_pk_seen;
                data_fp = o.data_fp; o.data_fp = nullptr;
                data_end_offset = o.data_end_offset;
                data_dirty = o.data_dirty;
                pk_index_valid = o.pk_index_valid;
                // spin: leave as-is (already unlocked for a freshly inserted entry)
            }
            return *this;
        }
        TableRuntime(const TableRuntime &) = delete;
        TableRuntime &operator=(const TableRuntime &) = delete;
    };

    std::unordered_map<std::string, TableRuntime> runtime; // key: UPPER(table)
    std::unordered_map<std::string, std::unique_ptr<std::mutex>> table_mu; // key: UPPER(table)

    struct CacheEntry {
        std::string key;
        std::vector<std::string> rows;
        size_t bytes = 0;
    };
    // LRU cache of small SELECT result sets. Strict memory bound.
    std::list<CacheEntry> cache_lru;
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> cache_map;
    size_t cache_bytes = 0;
    size_t cache_bytes_limit = 16 * 1024 * 1024; // 16MB default cap

    std::mutex mu;

    std::ofstream wal;
    size_t wal_unflushed_bytes = 0;
    bool wal_enabled = false;
    bool replaying = false;

    // ---- Performance optimizations ----
    std::atomic<bool> cache_dirty{false};

    // WAL batch buffer
    std::string wal_batch_buf;
    size_t wal_batch_count = 0;
    static constexpr size_t WAL_BATCH_BYTES = 4u * 1024u * 1024u;
    static constexpr size_t WAL_BATCH_COUNT = 8192;

    // Per-table write arena (front buffer — written by insert threads)
    std::unordered_map<std::string, std::string> write_arenas;
    static constexpr size_t WRITE_ARENA_FLUSH = 8u * 1024u * 1024u;

    // ---- Async background writer ----
    // The writer thread drains swap_arenas to disk while insert threads
    // continue filling write_arenas. This decouples disk latency from
    // insert throughput entirely.
    struct AsyncWriter {
        std::mutex              mu;
        std::condition_variable cv;
        // swap_arenas: filled by main thread under mu, drained by writer thread
        std::unordered_map<std::string, std::string> swap_arenas;
        // file handles mirrored from parent Database::runtime (writer uses these)
        std::unordered_map<std::string, FILE*> file_handles;
        bool stop = false;
        bool has_work = false;
        std::thread thread;
    } async_writer;
};

static Database g_db;

// ---- Async background writer implementation ----
static void async_writer_thread_fn() {
    auto &aw = g_db.async_writer;
    std::unordered_map<std::string, std::string> local_arenas;

    while (true) {
        {
            std::unique_lock<std::mutex> lk(aw.mu);
            aw.cv.wait(lk, [&]{ return aw.has_work || aw.stop; });
            if (aw.stop && !aw.has_work) break;
            local_arenas.swap(aw.swap_arenas);
            aw.has_work = false;
        }
        // Write all pending arenas to disk outside the lock (non-blocking for inserts).
        for (auto &kv : local_arenas) {
            if (kv.second.empty()) continue;
            auto fit = aw.file_handles.find(kv.first);
            if (fit == aw.file_handles.end() || !fit->second) continue;
            (void)std::fwrite(kv.second.data(), 1, kv.second.size(), fit->second);
            kv.second.clear();
        }
        // Notify anyone waiting in flush_all_pending that this batch is done.
        {
            std::lock_guard<std::mutex> done_lk(aw.mu);
            // has_work already cleared above; notify waiters.
        }
        aw.cv.notify_all();
    }
    // Final flush: drain any remaining data.
    for (auto &kv : local_arenas) {
        if (kv.second.empty()) continue;
        auto fit = aw.file_handles.find(kv.first);
        if (fit != aw.file_handles.end() && fit->second) {
            (void)std::fwrite(kv.second.data(), 1, kv.second.size(), fit->second);
            (void)std::fflush(fit->second);
        }
    }
}

static void start_async_writer() {
    g_db.async_writer.stop = false;
    g_db.async_writer.has_work = false;
    g_db.async_writer.thread = std::thread(async_writer_thread_fn);
}

static void stop_async_writer() {
    {
        std::lock_guard<std::mutex> lk(g_db.async_writer.mu);
        g_db.async_writer.stop = true;
        g_db.async_writer.has_work = true;
    }
    g_db.async_writer.cv.notify_one();
    if (g_db.async_writer.thread.joinable())
        g_db.async_writer.thread.join();
}

// Swap write_arenas into the async writer's queue and wake it.
// Called under table spinlock when arena reaches flush threshold.
// The insert thread continues immediately; the writer thread does disk I/O.
static void flush_arena_to_async_writer(const std::string &tkey, std::string &arena, FILE *fp) {
    auto &aw = g_db.async_writer;
    std::lock_guard<std::mutex> lk(aw.mu);
    // Append arena contents to swap_arenas[tkey] (may have residual from last swap).
    auto &slot = aw.swap_arenas[tkey];
    if (slot.empty()) {
        slot.swap(arena);   // zero-copy: just swap pointers
    } else {
        slot.append(arena);
        arena.clear();
    }
    aw.file_handles[tkey] = fp;
    aw.has_work = true;
    aw.cv.notify_one();
    // arena is now empty; reserve capacity for next batch.
    arena.reserve(Database::WRITE_ARENA_FLUSH + 4096);
}

// ---- SPSC lock-free double-buffer pipeline ----
// Reader thread: recv() + memchr scan -> pushes chunks of raw SQL bytes
// Parser thread: pops chunks -> parses INSERTs -> serializes to arena
//
// Uses a double-buffer so reader fills buf[1] while parser processes buf[0].
struct PipelineChunk {
    static constexpr size_t BUF_SIZE = 256 * 1024;
    char   data[BUF_SIZE];
    size_t len = 0;
    bool   eof = false;   // signals parser to stop
};

struct PipelinePair {
    PipelineChunk bufs[2];
    // write_idx: index reader is currently filling (0 or 1)
    // read_idx:  index parser is currently consuming (0 or 1)
    std::atomic<int>  full[2];   // 1 = chunk ready for parser, 0 = empty
    std::atomic<bool> done{false};

    PipelinePair() {
        full[0].store(0, std::memory_order_relaxed);
        full[1].store(0, std::memory_order_relaxed);
    }

    // Reader: get buffer to write into (spin until parser finishes with it)
    PipelineChunk *writer_get(int idx) {
        int spin = 0;
        while (full[idx].load(std::memory_order_acquire) != 0) {
            if (++spin > 2000) { std::this_thread::yield(); spin = 0; }
        }
        return &bufs[idx];
    }
    // Reader: mark buffer as ready for parser
    void writer_commit(int idx) {
        full[idx].store(1, std::memory_order_release);
    }
    // Parser: get buffer to read (spin until reader fills it)
    PipelineChunk *reader_get(int idx) {
        int spin = 0;
        while (full[idx].load(std::memory_order_acquire) == 0) {
            if (done.load(std::memory_order_relaxed) && full[idx].load(std::memory_order_acquire) == 0)
                return nullptr;
            if (++spin > 2000) { std::this_thread::yield(); spin = 0; }
        }
        return &bufs[idx];
    }
    // Parser: release buffer back to reader
    void reader_release(int idx) {
        full[idx].store(0, std::memory_order_release);
    }
};

// Parse and serialize a batch of INSERT string_views into a local arena.
// Thread-safe: operates only on the provided local_arena (no shared state writes).
// Returns number of rows successfully inserted.
static size_t parse_and_serialize_batch(
    const std::vector<std::string_view> &stmts,
    const TableSchema &schema,
    std::string &local_arena)
{
    static constexpr std::string_view INSERT_PREFIX = "INSERT INTO ";
    std::vector<std::string_view> vals;
    vals.reserve(schema.columns.size());
    size_t count = 0;

    for (auto sv_sql : stmts) {
        // Trim and strip trailing semicolon
        while (!sv_sql.empty() && (unsigned char)sv_sql.front() <= ' ') sv_sql.remove_prefix(1);
        if (sv_sql.empty()) continue;
        std::string_view sv_no_semi = sv_sql;
        if (!sv_no_semi.empty() && sv_no_semi.back() == ';') sv_no_semi.remove_suffix(1);
        while (!sv_no_semi.empty() && (unsigned char)sv_no_semi.back() <= ' ') sv_no_semi.remove_suffix(1);

        std::string_view table_sv;
        std::string sv_err2;
        vals.clear();
        if (!fast_parse_insert_single_row_views_sv(sv_no_semi, table_sv, vals, sv_err2)) continue;
        if (vals.size() != schema.columns.size()) continue;

        // Serialize into local_arena
        uint32_t ncols = (uint32_t)vals.size();
        size_t row_bytes = sizeof(uint32_t);
        for (auto v : vals) row_bytes += sizeof(uint32_t) + v.size();
        size_t wp = local_arena.size();
        local_arena.resize(wp + row_bytes);
        char *dst = local_arena.data() + wp;
        std::memcpy(dst, &ncols, sizeof(ncols)); dst += sizeof(ncols);
        for (auto v : vals) {
            uint32_t len = (uint32_t)v.size();
            std::memcpy(dst, &len, sizeof(len)); dst += sizeof(len);
            if (len > 0) { std::memcpy(dst, v.data(), len); dst += len; }
        }
        count++;
    }
    return count;
}

// RAII spinlock guard using atomic_flag.
// Spins briefly then yields to avoid monopolising CPU under contention.
struct SpinGuard {
    std::atomic_flag &flag;
    explicit SpinGuard(std::atomic_flag &f) : flag(f) {
        int spin = 0;
        while (flag.test_and_set(std::memory_order_acquire)) {
            if (++spin > 1000) {
                std::this_thread::yield();
                spin = 0;
            }
        }
    }
    ~SpinGuard() { flag.clear(std::memory_order_release); }
};

static std::mutex &get_table_mutex_locked(const std::string &tkey) {
    auto it = g_db.table_mu.find(tkey);
    if (it != g_db.table_mu.end() && it->second) {
        return *it->second;
    }
    auto ins = g_db.table_mu.emplace(tkey, std::make_unique<std::mutex>());
    return *ins.first->second;
}

static void send_text(int client_socket, const std::string &message) {
    const char *data = message.c_str();
    size_t len = message.size();
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(client_socket, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (n == 0) {
            break;
        }
        sent += (size_t)n;
    }
}

static void flush_runtime_table_file(const std::string &table_name) {
    std::string tkey = upper(table_name);
    auto it = g_db.runtime.find(tkey);
    if (it == g_db.runtime.end()) {
        return;
    }
    auto &rt = it->second;
    // Flush arena first, then FILE* buffer.
    auto ait = g_db.write_arenas.find(tkey);
    if (ait != g_db.write_arenas.end() && !ait->second.empty() && rt.data_fp) {
        std::string dummy_err;
        (void)std::fwrite(ait->second.data(), 1, ait->second.size(), rt.data_fp);
        ait->second.clear();
        rt.data_dirty = true;
    }
    if (rt.data_fp && rt.data_dirty) {
        (void)std::fflush(rt.data_fp);
        rt.data_dirty = false;
    }
}

static bool wal_flush_batch();  // forward declaration

// Flush all pending arenas + WAL batch. Call when client disconnects or before reads.
static void flush_all_pending() {
    // Push any remaining arena data to async writer.
    for (auto &kv : g_db.write_arenas) {
        if (kv.second.empty()) continue;
        auto rit = g_db.runtime.find(kv.first);
        if (rit == g_db.runtime.end() || !rit->second.data_fp) continue;
        flush_arena_to_async_writer(kv.first, kv.second, rit->second.data_fp);
        rit->second.data_dirty = true;
    }
    // Synchronise with async writer: wait until it finishes current batch,
    // then flush file handles to ensure data is on disk before reads.
    {
        auto &aw = g_db.async_writer;
        // Signal the writer to flush, then wait for it to finish.
        {
            std::lock_guard<std::mutex> lk(aw.mu);
            aw.has_work = true;   // ensure writer wakes if idle
        }
        aw.cv.notify_one();

        // Spin-wait until writer confirms it has drained all pending work.
        // We use a second atomic flag to avoid the deadlock of waiting under lock.
        std::unique_lock<std::mutex> lk(aw.mu);
        aw.cv.wait(lk, [&]{ return !aw.has_work; });
    }
    // Now fflush all open file handles.
    for (auto &kv : g_db.runtime) {
        if (kv.second.data_fp && kv.second.data_dirty) {
            (void)std::fflush(kv.second.data_fp);
            kv.second.data_dirty = false;
        }
    }
    wal_flush_batch();
}

// Flush WAL batch buffer to disk. Caller must hold g_db.mu.
static bool wal_flush_batch() {
    if (!g_db.wal_enabled || g_db.wal_batch_buf.empty()) return true;
    if (!g_db.wal.is_open()) return false;
    g_db.wal.write(g_db.wal_batch_buf.data(), (std::streamsize)g_db.wal_batch_buf.size());
    g_db.wal.flush();
    g_db.wal_batch_buf.clear();
    g_db.wal_batch_count = 0;
    return (bool)g_db.wal;
}

static bool wal_append(const std::string &sql) {
    if (!g_db.wal_enabled) return true;
    if (g_db.replaying) return true;
    if (!g_db.wal.is_open()) return false;
    uint32_t len = (uint32_t)sql.size();
    uint32_t crc = crc32(reinterpret_cast<const uint8_t *>(sql.data()), sql.size());
    // Batch into memory; flush only when threshold is reached.
    g_db.wal_batch_buf.append(reinterpret_cast<const char *>(&len), sizeof(len));
    g_db.wal_batch_buf.append(sql.data(), sql.size());
    g_db.wal_batch_buf.append(reinterpret_cast<const char *>(&crc), sizeof(crc));
    g_db.wal_batch_count++;
    if (g_db.wal_batch_buf.size() >= Database::WAL_BATCH_BYTES ||
        g_db.wal_batch_count >= Database::WAL_BATCH_COUNT) {
        return wal_flush_batch();
    }
    return true;
}

static void send_row(int sock, const std::vector<std::string> &col_names, const std::vector<std::string> &col_values) {
    if (sock < 0) {
        return;
    }
    std::string row = "ROW ";
    row += std::to_string((int)col_values.size());
    row += " ";
    for (size_t i = 0; i < col_values.size(); i++) {
        const std::string &name = col_names[i];
        const std::string &value = col_values[i];
        row += std::to_string(name.size());
        row += ":";
        row += name;
        row += std::to_string(value.size());
        row += ":";
        row += value;
    }
    row += "\n";
    send_text(sock, row);
}

static bool eval_condition(const Condition &cond, const std::vector<std::string> &col_names, const std::vector<std::string> &col_values) {
    auto find_col = [&](const std::string &name) -> std::optional<std::string> {
        for (size_t i = 0; i < col_names.size(); i++) {
            if (upper(col_names[i]) == upper(name)) {
                return col_values[i];
            }
        }
        return std::nullopt;
    };

    std::optional<std::string> lhs_val = find_col(cond.lhs);
    if (!lhs_val.has_value()) {
        return false;
    }

    if (cond.rhs_is_string) {
        const std::string &a = lhs_val.value();
        const std::string &b = cond.rhs;
        if (cond.op == "=") return a == b;
        if (cond.op == ">") return a > b;
        if (cond.op == "<") return a < b;
        if (cond.op == ">=") return a >= b;
        if (cond.op == "<=") return a <= b;
        return false;
    }

    auto a = to_number(lhs_val.value());
    auto b = to_number(cond.rhs);
    if (!a.has_value() || !b.has_value()) {
        return false;
    }
    if (cond.op == "=") return a.value() == b.value();
    if (cond.op == ">") return a.value() > b.value();
    if (cond.op == "<") return a.value() < b.value();
    if (cond.op == ">=") return a.value() >= b.value();
    if (cond.op == "<=") return a.value() <= b.value();
    return false;
}

static bool table_read_row_at(const TableSchema &schema, uint64_t offset, std::vector<std::string> &out_row, std::string &err) {
    std::ifstream in(table_data_path(schema.name), std::ios::binary);
    if (!in.is_open()) {
        err = "Cannot open table file";
        return false;
    }
    in.seekg((std::streamoff)offset, std::ios::beg);
    if (!in) {
        err = "Bad row offset";
        return false;
    }
    uint32_t ncols = 0;
    in.read(reinterpret_cast<char *>(&ncols), sizeof(ncols));
    if (!in || ncols != schema.columns.size()) {
        err = "Corrupt table file";
        return false;
    }
    out_row.clear();
    out_row.reserve(ncols);
    for (uint32_t i = 0; i < ncols; i++) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char *>(&len), sizeof(len));
        if (!in) {
            err = "Corrupt table file";
            return false;
        }
        std::string v;
        v.resize(len);
        if (len > 0) {
            in.read(&v[0], (std::streamsize)len);
            if (!in) {
                err = "Corrupt table file";
                return false;
            }
        }
        out_row.push_back(std::move(v));
    }
    return true;
}

static bool table_append_row_views_rt(const TableSchema &schema, Database::TableRuntime &rt, const std::vector<std::string_view> &values, std::string &err, const std::string &tkey_hint = "");

static bool table_append_row_views(const TableSchema &schema, const std::vector<std::string_view> &values, std::string &err) {
    ensure_runtime_for_table(schema);
    auto it = g_db.runtime.find(upper(schema.name));
    if (it == g_db.runtime.end()) {
        err = "Table runtime missing";
        return false;
    }
    return table_append_row_views_rt(schema, it->second, values, err);
}

static bool table_append_row_rt(const TableSchema &schema, Database::TableRuntime &rt, const std::vector<std::string> &values, std::string &err);

static bool table_append_row(const TableSchema &schema, const std::vector<std::string> &values, std::string &err) {
    ensure_runtime_for_table(schema);
    auto &rt = g_db.runtime[upper(schema.name)];
    return table_append_row_rt(schema, rt, values, err);
}

static bool table_scan(const TableSchema &schema, const std::function<bool(const std::vector<std::string> &)> &on_row, std::string &err) {
    std::ifstream in(table_data_path(schema.name), std::ios::binary);
    if (!in.is_open()) {
        return true;
    }

    while (true) {
        uint32_t ncols = 0;
        in.read(reinterpret_cast<char *>(&ncols), sizeof(ncols));
        if (!in) {
            break;
        }
        if (ncols != schema.columns.size()) {
            break;
        }
        std::vector<std::string> row;
        row.reserve(ncols);
        bool truncated = false;
        for (uint32_t i = 0; i < ncols; i++) {
            uint32_t len = 0;
            in.read(reinterpret_cast<char *>(&len), sizeof(len));
            if (!in) {
                truncated = true;
                break;
            }
            std::string v;
            v.resize(len);
            if (len > 0) {
                in.read(&v[0], (std::streamsize)len);
                if (!in) {
                    truncated = true;
                    break;
                }
            }
            row.push_back(std::move(v));
        }
        if (truncated) {
            break;
        }
        if (!on_row(row)) {
            if (!err.empty()) {
                return false;
            }
            break;
        }
    }
    return true;
}

static bool row_is_expired(const TableSchema &schema, const std::vector<std::string> &row) {
    if (!schema.expires_at_col.has_value()) {
        return false;
    }
    size_t idx = schema.expires_at_col.value();
    if (idx >= row.size()) {
        return false;
    }
    auto n = to_number(row[idx]);
    if (!n.has_value()) {
        return false;
    }
    std::time_t now = std::time(nullptr);
    return (long double)now > n.value();
}

static bool catalog_load(std::string &err) {
    (void)err;
    return true;
}

static bool rebuild_pk_index_for_table(const TableSchema &schema, std::string &err) {
    std::string tkey = upper(schema.name);
    auto rit = g_db.runtime.find(tkey);
    if (rit == g_db.runtime.end()) return true;
    auto &rt = rit->second;
    if (!rt.has_int_pk_index) return true;
    if (rt.pk_index_valid) return true;  // already up to date

    // Ensure async writer has flushed all pending data before we read the file.
    flush_all_pending();

    std::string path = table_data_path(schema.name);
    FILE *fp = std::fopen(path.c_str(), "rb");
    if (!fp) { rt.has_int_pk_index = false; return true; }

    long long max_pk = rt.max_pk_seen;
    if (max_pk <= 0 || max_pk > 20000000LL) {
        std::fclose(fp);
        if (max_pk > 20000000LL) rt.has_int_pk_index = false;
        rt.pk_index_valid = true;
        return true;
    }
    rt.pk_offsets.assign((size_t)max_pk + 1, 0);

    uint64_t offset = 0;
    size_t pk_col = rt.pk_col;
    size_t ncols_expected = schema.columns.size();
    while (true) {
        uint32_t ncols = 0;
        if (std::fread(&ncols, sizeof(ncols), 1, fp) != 1) break;
        if (ncols != (uint32_t)ncols_expected) break;
        uint64_t row_start = offset;
        offset += sizeof(uint32_t);
        long long pk = 0;
        bool got_pk = false;
        for (uint32_t c = 0; c < ncols; c++) {
            uint32_t len = 0;
            if (std::fread(&len, sizeof(len), 1, fp) != 1) goto done;
            offset += sizeof(uint32_t);
            if (c == (uint32_t)pk_col && len < 32) {
                char vbuf[32] = {};
                if (len > 0 && std::fread(vbuf, 1, len, fp) != len) goto done;
                offset += len;
                char *endp = nullptr;
                pk = std::strtoll(vbuf, &endp, 10);
                got_pk = true;
            } else {
                if (std::fseek(fp, (long)len, SEEK_CUR) != 0) goto done;
                offset += len;
            }
        }
        if (got_pk && pk > 0 && pk < (long long)rt.pk_offsets.size()) {
            rt.pk_offsets[(size_t)pk] = row_start;
            rt.max_pk_seen = std::max(rt.max_pk_seen, pk);
        }
    }
done:
    std::fclose(fp);
    rt.pk_index_valid = true;
    return true;
}

static ColType parse_type(const std::string &type_tok, int &varchar_len) {
    varchar_len = -1;
    std::string t = upper(trim(type_tok));
    if (t == "INT") return ColType::Int;
    if (t == "DECIMAL") return ColType::Decimal;
    if (t == "DATETIME") return ColType::Datetime;
    if (t.rfind("VARCHAR", 0) == 0) {
        size_t lp = t.find('(');
        size_t rp = t.find(')');
        if (lp != std::string::npos && rp != std::string::npos && rp > lp + 1) {
            varchar_len = std::atoi(t.substr(lp + 1, rp - lp - 1).c_str());
        }
        return ColType::Varchar;
    }
    return ColType::Unknown;
}

static bool exec_create_table(const std::string &sql, std::string &err) {
    size_t p = upper(sql).find("CREATE TABLE");
    if (p == std::string::npos) {
        err = "Malformed CREATE TABLE";
        return false;
    }
    std::string rest = trim(sql.substr(p + std::string("CREATE TABLE").size()));
    size_t lpar = rest.find('(');
    size_t rpar = rest.rfind(')');
    if (lpar == std::string::npos || rpar == std::string::npos || rpar <= lpar) {
        err = "Malformed CREATE TABLE";
        return false;
    }
    std::string table_name = trim(rest.substr(0, lpar));
    if (table_name.empty()) {
        err = "Missing table name";
        return false;
    }
    std::string cols = rest.substr(lpar + 1, rpar - lpar - 1);
    auto parts = split_csv_top_level(cols);
    if (parts.empty()) {
        err = "No columns";
        return false;
    }

    TableSchema schema;
    schema.name = table_name;
    for (const auto &part : parts) {
        std::stringstream ss(part);
        std::string col_name;
        ss >> col_name;
        std::string type_tok;
        ss >> type_tok;
        if (col_name.empty() || type_tok.empty()) {
            err = "Invalid column";
            return false;
        }
        Column col;
        col.name = col_name;
        int vlen = -1;
        col.type = parse_type(type_tok, vlen);
        col.varchar_len = vlen;
        if (col.type == ColType::Unknown) {
            err = "Unsupported type";
            return false;
        }
        if (col.type == ColType::Varchar && col.varchar_len <= 0) {
            err = "Invalid VARCHAR length";
            return false;
        }
        if (schema.col_index.find(upper(col.name)) != schema.col_index.end()) {
            err = "Duplicate column";
            return false;
        }
        schema.col_index[upper(col.name)] = schema.columns.size();
        schema.columns.push_back(col);
        if (upper(col.name) == "EXPIRES_AT") {
            schema.expires_at_col = schema.columns.size() - 1;
        }
    }

    if (!schema.expires_at_col.has_value()) {
        err = "Missing EXPIRES_AT column";
        return false;
    }

    g_db.schemas[upper(table_name)] = schema;
    g_db.runtime.erase(upper(table_name));
    ensure_runtime_for_table(schema);
    std::ofstream trunc(table_data_path(table_name), std::ios::binary | std::ios::out | std::ios::trunc);
    (void)trunc;

    return wal_append(sql + ";");
}

static bool fast_parse_insert_values(
    const std::string &sql,
    std::string &out_table,
    std::vector<std::vector<std::string>> &out_rows,
    std::string &err) {

    // Fast path for: INSERT INTO <table> VALUES (...),(...);
    // Handles quoted strings with '' escaping.
    std::string_view s(sql);

    auto skip_ws = [&](size_t &i) {
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) {
            i++;
        }
    };

    auto ci_match = [&](size_t i, std::string_view kw) -> bool {
        if (i + kw.size() > s.size()) return false;
        for (size_t k = 0; k < kw.size(); k++) {
            char a = s[i + k];
            char b = kw[k];
            if (a >= 'a' && a <= 'z') a = (char)(a - 'a' + 'A');
            if (b >= 'a' && b <= 'z') b = (char)(b - 'a' + 'A');
            if (a != b) return false;
        }
        return true;
    };

    size_t i = 0;
    skip_ws(i);
    if (!ci_match(i, "INSERT")) {
        return false;
    }
    i += 6;
    skip_ws(i);
    if (!ci_match(i, "INTO")) {
        return false;
    }
    i += 4;
    skip_ws(i);

    // table name
    size_t t0 = i;
    while (i < s.size() && is_ident_char(s[i])) {
        i++;
    }
    if (i == t0) {
        return false;
    }
    out_table.assign(s.substr(t0, i - t0));
    skip_ws(i);
    if (!ci_match(i, "VALUES")) {
        return false;
    }
    i += 6;
    skip_ws(i);

    out_rows.clear();

    auto parse_value = [&](size_t &p, std::string &out_val, bool &out_is_str) -> bool {
        skip_ws(p);
        if (p >= s.size()) return false;
        if (s[p] == '\'') {
            out_is_str = true;
            p++;
            std::string v;
            while (p < s.size()) {
                char c = s[p];
                if (c == '\'') {
                    if (p + 1 < s.size() && s[p + 1] == '\'') {
                        v.push_back('\'');
                        p += 2;
                        continue;
                    }
                    p++;
                    out_val = std::move(v);
                    return true;
                }
                v.push_back(c);
                p++;
            }
            return false;
        }

        out_is_str = false;
        size_t start = p;
        while (p < s.size()) {
            char c = s[p];
            if (c == ',' || c == ')' || c == ';' || c == '\n' || c == '\r') {
                break;
            }
            p++;
        }
        size_t end = p;
        while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t')) {
            end--;
        }
        while (start < end && (s[start] == ' ' || s[start] == '\t')) {
            start++;
        }
        if (end <= start) return false;
        out_val.assign(s.substr(start, end - start));
        return true;
    };

    while (i < s.size()) {
        skip_ws(i);
        if (i >= s.size()) break;
        if (s[i] == ';') {
            break;
        }
        if (s[i] != '(') {
            return false;
        }
        i++; // '(' 

        std::vector<std::string> row;
        while (true) {
            std::string v;
            bool is_str = false;
            if (!parse_value(i, v, is_str)) {
                return false;
            }
            row.push_back(std::move(v));

            skip_ws(i);
            if (i >= s.size()) return false;
            if (s[i] == ',') {
                i++;
                continue;
            }
            if (s[i] == ')') {
                i++;
                break;
            }
            return false;
        }

        out_rows.push_back(std::move(row));
        skip_ws(i);
        if (i < s.size() && s[i] == ',') {
            i++;
            continue;
        }
        if (i < s.size() && s[i] == ';') {
            break;
        }
    }

    if (out_rows.empty()) {
        err = "Malformed VALUES";
        return false;
    }
    return true;
}

static std::string table_data_path(const std::string &table_name);

static bool table_read_row_at_projected(const TableSchema &schema,
                                       uint64_t offset,
                                       const std::vector<size_t> &proj_indices,
                                       std::vector<std::string> &out_projected,
                                       std::optional<std::string> &out_expires_at,
                                       std::string &err) {
    std::ifstream in(table_data_path(schema.name), std::ios::binary);
    if (!in.is_open()) {
        err = "Cannot open table file";
        return false;
    }
    in.seekg((std::streamoff)offset, std::ios::beg);
    if (!in) {
        err = "Bad row offset";
        return false;
    }
    uint32_t ncols = 0;
    in.read(reinterpret_cast<char *>(&ncols), sizeof(ncols));
    if (!in || ncols != schema.columns.size()) {
        err = "Corrupt table file";
        return false;
    }

    std::vector<int32_t> pos;
    pos.assign(ncols, -1);
    for (size_t i = 0; i < proj_indices.size(); i++) {
        if (proj_indices[i] >= (size_t)ncols) {
            err = "Corrupt table file";
            return false;
        }
        pos[proj_indices[i]] = (int32_t)i;
    }
    std::optional<size_t> expires_idx = std::nullopt;
    if (schema.expires_at_col.has_value()) {
        size_t eidx = schema.expires_at_col.value();
        if (eidx < (size_t)ncols) {
            expires_idx = eidx;
        }
    }

    out_projected.clear();
    out_projected.resize(proj_indices.size());
    out_expires_at.reset();

    for (uint32_t i = 0; i < ncols; i++) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char *>(&len), sizeof(len));
        if (!in) {
            err = "Corrupt table file";
            return false;
        }
        int32_t p = pos[i];
        bool want_expires = expires_idx.has_value() && i == expires_idx.value();
        if (p < 0 && !want_expires) {
            in.seekg((std::streamoff)len, std::ios::cur);
            if (!in) {
                err = "Corrupt table file";
                return false;
            }
            continue;
        }

        std::string v;
        v.resize(len);
        if (len > 0) {
            in.read(&v[0], (std::streamsize)len);
            if (!in) {
                err = "Corrupt table file";
                return false;
            }
        }

        if (p >= 0) {
            out_projected[(size_t)p] = std::move(v);
        } else {
            out_expires_at = std::move(v);
        }
    }
    return true;
}

static void ensure_runtime_for_table(const TableSchema &schema);
static std::string table_data_path(const std::string &table_name);
static bool parse_int64_strict_sv(std::string_view sv, long long &out);

static bool table_flush_arena(const std::string &tkey, Database::TableRuntime &rt, std::string &err) {
    auto it = g_db.write_arenas.find(tkey);
    if (it == g_db.write_arenas.end() || it->second.empty()) return true;
    if (!rt.data_fp) {
        err = "Cannot flush arena: file not open";
        return false;
    }
    const std::string &arena = it->second;
    if (std::fwrite(arena.data(), 1, arena.size(), rt.data_fp) != arena.size()) {
        err = "Arena write failed";
        return false;
    }
    it->second.clear();
    rt.data_dirty = true;
    return true;
}

static bool table_append_row_views_rt(const TableSchema &schema, Database::TableRuntime &rt, const std::vector<std::string_view> &values, std::string &err, const std::string &tkey_hint) {
    if (!rt.data_fp) {
        // Fallback: attempt open now.
        std::string path = table_data_path(schema.name);
        rt.data_fp = std::fopen(path.c_str(), "ab+");
        rt.data_end_offset = 0;
        if (rt.data_fp) {
            (void)::fseek(rt.data_fp, 0, SEEK_END);
            long pos = ::ftell(rt.data_fp);
            if (pos >= 0) {
                rt.data_end_offset = (uint64_t)pos;
            }
            // Large kernel buffer: let OS batch the writes.
            (void)std::setvbuf(rt.data_fp, nullptr, _IOFBF, 8 << 20);
            // Register with async writer so it can flush to this handle.
            register_file_with_async_writer(upper(schema.name), rt.data_fp);
        }
    }
    if (!rt.data_fp) {
        err = "Cannot open table file";
        return false;
    }

    uint64_t offset = rt.data_end_offset;

    // Serialize row into per-table arena buffer using a single append per row.
    // Pre-build the binary record in a stack buffer then copy once to arena.
    const std::string &arena_key = tkey_hint.empty() ? upper(schema.name) : tkey_hint;
    std::string &arena = g_db.write_arenas[arena_key];
    if (arena.capacity() < Database::WRITE_ARENA_FLUSH) {
        arena.reserve(Database::WRITE_ARENA_FLUSH + 4096);
    }
    size_t row_start = arena.size();

    // Compute total row size first, then reserve + write in one shot.
    uint32_t ncols = (uint32_t)values.size();
    size_t row_bytes = sizeof(uint32_t);  // ncols
    for (auto sv : values) row_bytes += sizeof(uint32_t) + sv.size();

    // Extend arena by row_bytes and write directly into it.
    size_t write_pos = arena.size();
    arena.resize(write_pos + row_bytes);
    char *dst = arena.data() + write_pos;

    std::memcpy(dst, &ncols, sizeof(ncols)); dst += sizeof(ncols);
    for (auto sv : values) {
        uint32_t len = (uint32_t)sv.size();
        std::memcpy(dst, &len, sizeof(len)); dst += sizeof(len);
        if (len > 0) { std::memcpy(dst, sv.data(), len); dst += len; }
    }

    size_t row_size = arena.size() - row_start;
    rt.data_end_offset += (uint64_t)row_size;

    // When arena reaches threshold, hand it off to async writer (non-blocking).
    if (arena.size() >= Database::WRITE_ARENA_FLUSH) {
        flush_arena_to_async_writer(arena_key, arena, rt.data_fp);
        rt.data_dirty = true;
    }

    // Deferred PK index: only track max_pk during bulk inserts.
    // pk_offsets is rebuilt lazily on the first SELECT that needs it.
    // This avoids 80MB of cache-cold random writes for 10M sequential inserts.
    if (rt.has_int_pk_index && rt.pk_col < values.size()) {
        long long pk = 0;
        if (parse_int64_strict_sv(values[rt.pk_col], pk) && pk > 0) {
            if (pk > 20'000'000LL) {
                rt.has_int_pk_index = false;
            } else {
                rt.max_pk_seen = std::max(rt.max_pk_seen, pk);
                rt.pk_index_valid = false;  // mark stale; rebuild on next SELECT
            }
        }
    }

    return true;
}

static bool fast_parse_insert_single_row_views_sv(
    std::string_view sql,
    std::string_view &out_table,
    std::vector<std::string_view> &out_vals,
    std::string &err);

static bool fast_parse_insert_single_row_views(
    const std::string &sql,
    std::string &out_table,
    std::vector<std::string_view> &out_vals,
    std::string &err) {
    out_table.clear();
    out_vals.clear();

    std::string_view table_sv;
    bool ok = fast_parse_insert_single_row_views_sv(std::string_view(sql), table_sv, out_vals, err);
    if (!ok) {
        return false;
    }
    out_table.assign(table_sv);
    return true;
}

static bool fast_parse_insert_single_row_views_sv(
    std::string_view sql,
    std::string_view &out_table,
    std::vector<std::string_view> &out_vals,
    std::string &err) {
    out_table = std::string_view();
    out_vals.clear();

    std::string_view s(sql);
    s = trim_sv(s);
    if (!starts_with_ci(s, "INSERT INTO ")) {
        err = "not insert";
        return false;
    }

    s = s.substr(std::string_view("INSERT INTO ").size());
    s = trim_sv(s);
    if (s.empty()) {
        err = "Malformed INSERT";
        return false;
    }

    size_t i = 0;
    while (i < s.size() && is_ident_char(s[i])) {
        i++;
    }
    if (i == 0) {
        err = "Malformed INSERT";
        return false;
    }
    out_table = s.substr(0, i);
    s = trim_sv(s.substr(i));

    if (s.size() < 6) {
        err = "Malformed INSERT";
        return false;
    }
    if (!starts_with_ci(s.substr(0, 6), "VALUES")) {
        err = "Malformed INSERT";
        return false;
    }
    s = trim_sv(s.substr(6));
    if (s.empty() || s.front() != '(') {
        err = "Malformed VALUES";
        return false;
    }

    // Parse exactly one (...) group -- memchr for SIMD delimiter scanning.
    i = 0;
    if (s[i] != '(') { err = "Malformed VALUES"; return false; }
    i++; // skip '('

    while (i < s.size()) {
        while (i < s.size() && (unsigned char)s[i] <= ' ') { i++; }
        if (i >= s.size()) { err = "Malformed VALUES"; return false; }
        if (s[i] == ')') { i++; break; }

        if (s[i] == '\'') {
            // Quoted string: memchr finds closing quote (SIMD on modern CPUs).
            const char *qbase = s.data() + i + 1;
            size_t qavail = s.size() - (i + 1);
            const char *qend = (const char *)std::memchr(qbase, '\'', qavail);
            if (!qend) { err = "Unterminated string"; return false; }
            out_vals.push_back(s.substr(i + 1, (size_t)(qend - qbase)));
            i = (size_t)(qend - s.data()) + 1;
        } else {
            // Numeric/bare token: memchr to find ',' or ')'.
            const char *nbase = s.data() + i;
            size_t navail = s.size() - i;
            const char *d1 = (const char *)std::memchr(nbase, ',', navail);
            const char *d2 = (const char *)std::memchr(nbase, ')', navail);
            const char *delim = (!d1 || (d2 && d2 < d1)) ? d2 : d1;
            if (!delim || delim == nbase) { err = "Invalid value"; return false; }
            const char *vend = delim;
            while (vend > nbase && (unsigned char)*(vend-1) <= ' ') vend--;
            out_vals.push_back(s.substr(i, (size_t)(vend - nbase)));
            i = (size_t)(delim - s.data());
        }

        while (i < s.size() && (unsigned char)s[i] <= ' ') { i++; }
        if (i >= s.size()) { err = "Malformed VALUES"; return false; }
        if (s[i] == ',') { i++; continue; }
        if (s[i] == ')') { i++; break; }
        err = "Malformed VALUES"; return false;
    }

    // After the group: allow optional ';' and whitespace only.
    s = trim_sv(s.substr(i));
    if (!s.empty() && s.front() == ';') {
        s = trim_sv(s.substr(1));
    }
    if (!s.empty()) {
        // likely multi-row insert; let generic fast parser handle it.
        err = "Not a single-row insert";
        return false;
    }

    if (out_vals.empty()) {
        err = "Malformed VALUES";
        return false;
    }
    return true;
}

static std::string trim(const std::string &s) {
    size_t b = 0;
    while (b < s.size() && std::isspace((unsigned char)s[b])) {
        b++;
    }
    size_t e = s.size();
    while (e > b && std::isspace((unsigned char)s[e - 1])) {
        e--;
    }
    return s.substr(b, e - b);
}

static std::string upper(const std::string &s) {
    std::string r = s;
    for (char &c : r) {
        c = (char)std::toupper((unsigned char)c);
    }
    return r;
}

static std::string_view trim_sv(std::string_view sv) {
    size_t b = 0;
    while (b < sv.size() && std::isspace((unsigned char)sv[b])) {
        b++;
    }
    size_t e = sv.size();
    while (e > b && std::isspace((unsigned char)sv[e - 1])) {
        e--;
    }
    return sv.substr(b, e - b);
}

static bool starts_with_ci(std::string_view s, std::string_view prefix) {
    if (s.size() < prefix.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); i++) {
        char a = s[i];
        char b = prefix[i];
        if (std::toupper((unsigned char)a) != std::toupper((unsigned char)b)) {
            return false;
        }
    }
    return true;
}

static bool is_number_sv(std::string_view sv) {
    sv = trim_sv(sv);
    if (sv.empty()) {
        return false;
    }
    size_t i = 0;
    if (sv[i] == '-' || sv[i] == '+') {
        i++;
    }
    bool any = false;
    bool dot = false;
    for (; i < sv.size(); i++) {
        char c = sv[i];
        if (c >= '0' && c <= '9') {
            any = true;
            continue;
        }
        if (c == '.' && !dot) {
            dot = true;
            continue;
        }
        return false;
    }
    return any;
}

static bool parse_int64_strict_sv(std::string_view sv, long long &out) {
    sv = trim_sv(sv);
    if (sv.empty()) {
        return false;
    }
    bool neg = false;
    size_t i = 0;
    if (sv[0] == '-') {
        neg = true;
        i = 1;
    } else if (sv[0] == '+') {
        i = 1;
    }
    if (i >= sv.size()) {
        return false;
    }
    long long v = 0;
    for (; i < sv.size(); i++) {
        char c = sv[i];
        if (c < '0' || c > '9') {
            return false;
        }
        int d = c - '0';
        if (v > (LLONG_MAX - d) / 10) {
            return false;
        }
        v = v * 10 + d;
    }
    out = neg ? -v : v;
    return true;
}

static bool validate_expires_at_value_sv(std::string_view sv, std::string &err) {
    long long v = 0;
    if (!parse_int64_strict_sv(sv, v) || v <= 0) {
        err = "Invalid EXPIRES_AT";
        return false;
    }
    return true;
}

static std::vector<std::string> split_csv_top_level(const std::string &s) {
    std::vector<std::string> out;
    std::string cur;
    int paren = 0;
    bool in_str = false;
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        if (c == '\'' && (i == 0 || s[i - 1] != '\\')) {
            in_str = !in_str;
            cur.push_back(c);
            continue;
        }
        if (!in_str) {
            if (c == '(') {
                paren++;
            } else if (c == ')') {
                paren--;
            } else if (c == ',' && paren == 0) {
                out.push_back(trim(cur));
                cur.clear();
                continue;
            }
        }
        cur.push_back(c);
    }
    if (!cur.empty()) {
        out.push_back(trim(cur));
    }
    return out;
}

static bool parse_value_token(const std::string &token, std::string &out_value, bool &out_is_string) {
    std::string t = trim(token);
    if (t.size() >= 2 && t.front() == '\'' && t.back() == '\'') {
        out_is_string = true;
        out_value = t.substr(1, t.size() - 2);
        return true;
    }
    out_is_string = false;
    out_value = t;
    return true;
}

static std::optional<long double> to_number(const std::string &s) {
    char *end = nullptr;
    errno = 0;
    long double v = std::strtold(s.c_str(), &end);
    if (errno != 0 || end == s.c_str() || *end != '\0') {
        return std::nullopt;
    }
    return v;
}

static bool parse_where(const std::string &where_part, Condition &out_cond, std::string &err) {
    std::string s = trim(where_part);
    std::vector<std::string> ops = {">=", "<=", "=", ">", "<"};
    size_t op_pos = std::string::npos;
    std::string op;
    for (const auto &cand : ops) {
        size_t p = s.find(cand);
        if (p != std::string::npos) {
            op_pos = p;
            op = cand;
            break;
        }
    }
    if (op_pos == std::string::npos) {
        err = "Invalid WHERE condition";
        return false;
    }
    std::string lhs = trim(s.substr(0, op_pos));
    std::string rhs = trim(s.substr(op_pos + op.size()));
    if (lhs.empty() || rhs.empty()) {
        err = "Invalid WHERE condition";
        return false;
    }
    bool rhs_is_str = false;
    std::string rhs_val;
    if (!parse_value_token(rhs, rhs_val, rhs_is_str)) {
        err = "Invalid WHERE value";
        return false;
    }
    out_cond.lhs = lhs;
    out_cond.op = op;
    out_cond.rhs = rhs_val;
    out_cond.rhs_is_string = rhs_is_str;
    return true;
}

static uint32_t crc32(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

static bool ensure_data_dir() {
    // Best-effort: create data directory via system call to mkdir.
    // No external libs; rely on POSIX mkdir.
    ::mkdir(DATA_DIR, 0755);
    return true;
}

static void wipe_data_dir_best_effort() {
    DIR *d = ::opendir(DATA_DIR);
    if (!d) {
        return;
    }
    while (struct dirent *ent = ::readdir(d)) {
        const char *name = ent->d_name;
        if (!name || name[0] == '\0') {
            continue;
        }
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        std::string path = std::string(DATA_DIR) + "/" + name;
        (void)::unlink(path.c_str());
    }
    ::closedir(d);
}

static std::string table_data_path(const std::string &table_name) {
    return std::string(DATA_DIR) + "/" + table_name + ".data";
}

static void cache_clear() {
    g_db.cache_lru.clear();
    g_db.cache_map.clear();
    g_db.cache_bytes = 0;
    g_db.cache_dirty.store(false, std::memory_order_relaxed);
}

// Mark cache as dirty (fast path for inserts - no actual clear needed).
static void cache_invalidate() {
    g_db.cache_dirty.store(true, std::memory_order_relaxed);
}

// Called before any SELECT to flush dirty cache if needed. Caller holds g_db.mu.
static void cache_check_dirty() {
    if (g_db.cache_dirty.load(std::memory_order_relaxed)) {
        cache_clear();
    }
}

static void cache_put(const std::string &key, std::vector<std::string> rows, size_t bytes) {
    if (bytes > g_db.cache_bytes_limit) {
        return;
    }
    auto it = g_db.cache_map.find(key);
    if (it != g_db.cache_map.end()) {
        g_db.cache_bytes -= it->second->bytes;
        g_db.cache_lru.erase(it->second);
        g_db.cache_map.erase(it);
    }

    while (g_db.cache_bytes + bytes > g_db.cache_bytes_limit && !g_db.cache_lru.empty()) {
        auto &back = g_db.cache_lru.back();
        g_db.cache_bytes -= back.bytes;
        g_db.cache_map.erase(back.key);
        g_db.cache_lru.pop_back();
    }

    g_db.cache_lru.push_front(Database::CacheEntry{key, std::move(rows), bytes});
    g_db.cache_map[key] = g_db.cache_lru.begin();
    g_db.cache_bytes += bytes;
}

static bool cache_get(const std::string &key, std::vector<std::string> &out_rows) {
    auto it = g_db.cache_map.find(key);
    if (it == g_db.cache_map.end()) {
        return false;
    }
    // Move to front.
    g_db.cache_lru.splice(g_db.cache_lru.begin(), g_db.cache_lru, it->second);
    out_rows = it->second->rows;
    return true;
}

static bool parse_int64_strict(const std::string &s, long long &out) {
    std::string t = trim(s);
    if (t.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    long long v = std::strtoll(t.c_str(), &end, 10);
    if (errno != 0 || end == t.c_str() || *end != '\0') {
        return false;
    }
    out = v;
    return true;
}

static bool validate_expires_at_value(const std::string &s, std::string &err) {
    long long v = 0;
    if (!parse_int64_strict(s, v) || v <= 0) {
        err = "Invalid EXPIRES_AT";
        return false;
    }
    return true;
}

static bool validate_insert_row_values_sv(const TableSchema &schema, const std::vector<std::string_view> &values, std::string &err) {
    if (values.size() != schema.columns.size()) {
        err = "Column count mismatch";
        return false;
    }
    if (!schema.expires_at_col.has_value()) {
        err = "Missing EXPIRES_AT column";
        return false;
    }
    size_t eidx = schema.expires_at_col.value();
    if (eidx >= values.size()) {
        err = "Invalid EXPIRES_AT column";
        return false;
    }
    if (!validate_expires_at_value_sv(values[eidx], err)) {
        return false;
    }

    for (size_t i = 0; i < values.size(); i++) {
        const auto &col = schema.columns[i];
        std::string_view v = values[i];
        switch (col.type) {
            case ColType::Int: {
                long long out = 0;
                if (!parse_int64_strict_sv(v, out)) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Decimal: {
                std::string tmp(v);
                if (!to_number(tmp).has_value()) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Varchar: {
                if (col.varchar_len <= 0) {
                    err = "Invalid VARCHAR length";
                    return false;
                }
                if ((int)v.size() > col.varchar_len) {
                    err = "VARCHAR length exceeded";
                    return false;
                }
                break;
            }
            case ColType::Datetime: {
                long long out = 0;
                if (!parse_int64_strict_sv(v, out)) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Unknown:
            default:
                break;
        }
    }
    return true;
}

static bool validate_insert_row_values(const TableSchema &schema, const std::vector<std::string> &values, std::string &err) {
    if (values.size() != schema.columns.size()) {
        err = "Column count mismatch";
        return false;
    }
    if (!schema.expires_at_col.has_value()) {
        err = "Missing EXPIRES_AT column";
        return false;
    }
    size_t eidx = schema.expires_at_col.value();
    if (eidx >= values.size()) {
        err = "Invalid EXPIRES_AT column";
        return false;
    }
    if (!validate_expires_at_value(values[eidx], err)) {
        return false;
    }

    for (size_t i = 0; i < values.size(); i++) {
        const auto &col = schema.columns[i];
        const std::string &v = values[i];
        switch (col.type) {
            case ColType::Int: {
                long long out = 0;
                if (!parse_int64_strict(v, out)) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Decimal: {
                if (!to_number(v).has_value()) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Varchar: {
                if (col.varchar_len <= 0) {
                    err = "Invalid VARCHAR length";
                    return false;
                }
                if ((int)v.size() > col.varchar_len) {
                    err = "VARCHAR length exceeded";
                    return false;
                }
                break;
            }
            case ColType::Datetime: {
                long long out = 0;
                if (!parse_int64_strict(v, out)) {
                    err = "Type mismatch";
                    return false;
                }
                break;
            }
            case ColType::Unknown:
            default:
                break;
        }
    }
    return true;
}

static bool is_probably_pk_column(const TableSchema &schema, size_t col_idx) {
    if (col_idx >= schema.columns.size()) {
        return false;
    }
    if (col_idx == 0) {
        return true;
    }
    return upper(schema.columns[col_idx].name) == "ID";
}

static bool compare_values(const std::string &lhs, const std::string &rhs, const std::string &op) {
    // Prefer numeric compare if both sides are numeric.
    auto a = to_number(lhs);
    auto b = to_number(rhs);
    if (a.has_value() && b.has_value()) {
        if (op == "=") return a.value() == b.value();
        if (op == ">") return a.value() > b.value();
        if (op == "<") return a.value() < b.value();
        if (op == ">=") return a.value() >= b.value();
        if (op == "<=") return a.value() <= b.value();
        return false;
    }

    // Fallback lexicographic.
    if (op == "=") return lhs == rhs;
    if (op == ">") return lhs > rhs;
    if (op == "<") return lhs < rhs;
    if (op == ">=") return lhs >= rhs;
    if (op == "<=") return lhs <= rhs;
    return false;
}

static void register_file_with_async_writer(const std::string &tkey, FILE *fp) {
    std::lock_guard<std::mutex> lk(g_db.async_writer.mu);
    g_db.async_writer.file_handles[tkey] = fp;
}

static void ensure_runtime_for_table(const TableSchema &schema) {
    std::string tkey = upper(schema.name);
    if (g_db.runtime.find(tkey) != g_db.runtime.end()) {
        return;
    }
    Database::TableRuntime rt;
    rt.pk_offsets.resize(1, 0);
    rt.has_int_pk_index = false;
    rt.pk_col = 0;
    rt.data_fp = nullptr;
    rt.data_end_offset = 0;
    rt.data_dirty = false;

    // Enable PK offset index when the schema has a probable INT primary key.
    for (size_t i = 0; i < schema.columns.size(); i++) {
        if (!is_probably_pk_column(schema, i)) {
            continue;
        }
        if (schema.columns[i].type != ColType::Int) {
            continue;
        }
        rt.has_int_pk_index = true;
        rt.pk_col = i;
        rt.pk_index_valid = false;  // mark stale; rebuild on next SELECT
    }

    g_db.runtime[tkey] = std::move(rt);
    (void)get_table_mutex_locked(tkey);
}

static bool table_append_row_rt(const TableSchema &schema, Database::TableRuntime &rt, const std::vector<std::string> &values, std::string &err) {
    if (!rt.data_fp) {
        // Fallback: attempt open now.
        std::string path = table_data_path(schema.name);
        rt.data_fp = std::fopen(path.c_str(), "ab+");
        rt.data_end_offset = 0;
        if (rt.data_fp) {
            (void)::fseek(rt.data_fp, 0, SEEK_END);
            long pos = ::ftell(rt.data_fp);
            if (pos >= 0) {
                rt.data_end_offset = (uint64_t)pos;
            }
            (void)std::setvbuf(rt.data_fp, nullptr, _IOFBF, 1 << 20);
        }
    }
    if (!rt.data_fp) {
        err = "Cannot open table file";
        return false;
    }

    uint64_t offset = rt.data_end_offset;

    std::string buf;
    buf.reserve(sizeof(uint32_t) + values.size() * sizeof(uint32_t));

    uint32_t ncols = (uint32_t)values.size();
    buf.append(reinterpret_cast<const char *>(&ncols), sizeof(ncols));
    for (const auto &v : values) {
        uint32_t len = (uint32_t)v.size();
        buf.append(reinterpret_cast<const char *>(&len), sizeof(len));
        if (len > 0) {
            buf.append(v.data(), len);
        }
    }

    if (!buf.empty()) {
        if (std::fwrite(buf.data(), 1, buf.size(), rt.data_fp) != buf.size()) {
            err = "Write failed";
            return false;
        }
        rt.data_end_offset += (uint64_t)buf.size();
        rt.data_dirty = true;
    }

    // Update runtime PK index if configured.
    if (rt.has_int_pk_index && rt.pk_col < values.size()) {
        long long pk = 0;
        if (parse_int64_strict(values[rt.pk_col], pk) && pk > 0) {
            if (pk > (long long)rt.pk_offsets.size() - 1) {
                if (pk > 20'000'000LL) {
                    rt.has_int_pk_index = false;
                } else {
                    size_t new_cap = (size_t)pk + 1;
                    if (new_cap < rt.pk_offsets.capacity() * 2)
                        new_cap = rt.pk_offsets.capacity() * 2;
                    if (new_cap > 20'000'001ULL) new_cap = 20'000'001ULL;
                    rt.pk_offsets.reserve(new_cap);
                    rt.pk_offsets.resize((size_t)pk + 1, 0);
                }
            }
            if (rt.has_int_pk_index && pk < (long long)rt.pk_offsets.size()) {
                rt.pk_offsets[(size_t)pk] = offset;
                rt.max_pk_seen = std::max(rt.max_pk_seen, pk);
            }
        }
    }
    return true;
}

// Multi-row direct-to-arena INSERT parser.
// Single-pass: parses each tuple left-to-right, writes directly to arena.
// Works for both single-row and multi-row batch INSERT.
static size_t parse_insert_direct_to_arena(
    std::string_view sv, size_t ncols, std::string &arena, size_t paren_offset = 0)
{
    const char *cur;
    if (paren_offset > 0 && paren_offset <= sv.size())
        cur = sv.data() + paren_offset - 1;
    else {
        cur = (const char*)std::memchr(sv.data(), '(', sv.size());
        if (!cur) return 0;
    }
    const char *end = sv.data() + sv.size();
    size_t total_written = 0;

    while (cur < end && *cur == '(') {
        struct F { const char *ptr; uint32_t len; };
        F fields[32]; size_t nf = 0;
        const char *p = cur + 1;

        while (p < end && nf < ncols) {
            while (p < end && (unsigned char)*p <= ' ') p++;
            if (p >= end || *p == ')') break;
            if (*p == 0x27) {
                const char *qs = p+1;
                const char *qe = (const char*)std::memchr(qs, 0x27, (size_t)(end-qs));
                if (!qe) return total_written;
                fields[nf++] = {qs, (uint32_t)(qe-qs)};
                p = qe+1;
            } else {
                const char *d1 = (const char*)std::memchr(p, ',', (size_t)(end-p));
                const char *d2 = (const char*)std::memchr(p, ')', (size_t)(end-p));
                const char *d = (!d1||(d2&&d2<d1)) ? d2 : d1;
                if (!d || d == p) return total_written;
                const char *ve = d;
                while (ve > p && (unsigned char)*(ve-1) <= ' ') ve--;
                fields[nf++] = {p, (uint32_t)(ve-p)};
                p = d;
            }
            while (p < end && (unsigned char)*p <= ' ') p++;
            if (p < end && *p == ',') p++;
        }

        if (nf != ncols || p >= end || *p != ')') return total_written;
        p++;

        // Optional correctness enforcement: if we can resolve schema by table name,
        // validate values before serializing. This prevents bulk fast paths from
        // bypassing schema enforcement.
        {
            std::string_view t = sv;
            t = trim_sv(t);
            if (starts_with_ci(t, "INSERT INTO ")) {
                t = trim_sv(t.substr(std::string_view("INSERT INTO ").size()));
                size_t ti = 0;
                while (ti < t.size() && is_ident_char(t[ti])) ti++;
                if (ti > 0) {
                    std::string tkey = upper(std::string(t.substr(0, ti)));
                    const TableSchema *schema = nullptr;
                    {
                        std::lock_guard<std::mutex> lk(g_db.mu);
                        auto it = g_db.schemas.find(tkey);
                        if (it != g_db.schemas.end()) {
                            schema = &it->second;
                        }
                    }
                    if (schema) {
                        std::vector<std::string_view> vals;
                        vals.reserve(nf);
                        for (size_t i = 0; i < nf; i++) {
                            vals.emplace_back(fields[i].ptr, (size_t)fields[i].len);
                        }
                        std::string verr;
                        if (!validate_insert_row_values_sv(*schema, vals, verr)) {
                            return total_written;
                        }
                    }
                }
            }
        }

        size_t row_size = sizeof(uint32_t);
        for (size_t i = 0; i < nf; i++) row_size += sizeof(uint32_t) + fields[i].len;
        size_t sp = arena.size();
        arena.resize(sp + row_size);
        char *dst = arena.data() + sp;
        uint32_t nc = (uint32_t)nf;
        std::memcpy(dst, &nc, 4); dst += 4;
        for (size_t i = 0; i < nf; i++) {
            std::memcpy(dst, &fields[i].len, 4); dst += 4;
            if (fields[i].len) { std::memcpy(dst, fields[i].ptr, fields[i].len); dst += fields[i].len; }
        }
        total_written += row_size;

        while (p < end && (unsigned char)*p <= ' ') p++;
        if (p >= end || *p == ';') break;
        if (*p != ',') break;
        p++;
        while (p < end && (unsigned char)*p <= ' ') p++;
        cur = p;
    }
    return total_written;
}

static bool exec_insert(const std::string &sql, std::string &err) {
    std::string table_name;
    std::vector<std::string_view> single_vals;
    std::string sv_err;
    bool single_view_ok = fast_parse_insert_single_row_views(sql, table_name, single_vals, sv_err);

    std::vector<std::vector<std::string>> parsed_rows;
    std::string perr;
    bool fast_ok = false;
    if (!single_view_ok) {
        fast_ok = fast_parse_insert_values(sql, table_name, parsed_rows, perr);
    }

    TableSchema *schema = nullptr;
    Database::TableRuntime *rt = nullptr;
    std::mutex *table_mu = nullptr;
    std::string tkey;
    {
        std::lock_guard<std::mutex> lk(g_db.mu);
        tkey = upper(table_name);
        auto it = g_db.schemas.find(tkey);
        if (it == g_db.schemas.end()) {
            err = "Table does not exist";
            return false;
        }
        schema = &it->second;
        ensure_runtime_for_table(*schema);
        rt = &g_db.runtime[tkey];
        table_mu = &get_table_mutex_locked(tkey);
    }

    // Lock only this table while doing append I/O + in-memory PK index updates.
    std::lock_guard<std::mutex> tlk(*table_mu);

    if (single_view_ok) {
        if (single_vals.size() != schema->columns.size()) {
            err = "Column count mismatch";
            return false;
        }
        if (!validate_insert_row_values_sv(*schema, single_vals, err)) {
            return false;
        }
        if (!table_append_row_views_rt(*schema, *rt, single_vals, err)) {
            return false;
        }
    } else {
        if (!fast_ok) {
            err = perr.empty() ? "Malformed VALUES" : perr;
            return false;
        }
        for (auto &row : parsed_rows) {
            if (row.size() != schema->columns.size()) {
                err = "Column count mismatch";
                return false;
            }
            if (!validate_insert_row_values(*schema, row, err)) {
                return false;
            }
            if (!table_append_row_rt(*schema, *rt, row, err)) {
                return false;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lk(g_db.mu);
        cache_invalidate();  // fast: just sets atomic flag, no actual clear
        if (!wal_append(sql + ";")) {
            err = "WAL append failed";
            return false;
        }
    }
    return true;
}

static bool exec_select(const std::string &sql, int client_socket, std::string &err) {
    // Flush all pending write arenas and WAL batch before reading data files.
    // This ensures data written but not yet flushed to FILE* is visible.
    flush_all_pending();
    // Flush dirty cache from any inserts before running a SELECT.
    cache_check_dirty();

    std::string row_outbuf;
    row_outbuf.reserve(256u * 1024u);
    static constexpr size_t kRowOutFlushBytes = 1u * 1024u * 1024u;
    auto flush_row_outbuf = [&]() {
        if (client_socket >= 0 && !row_outbuf.empty()) {
            send_text(client_socket, row_outbuf);
            row_outbuf.clear();
        }
    };

    std::string s = trim(sql);
    if (!starts_with_ci(s, "SELECT ")) {
        err = "Malformed SELECT";
        return false;
    }

    size_t from_pos = upper(s).find(" FROM ");
    if (from_pos == std::string::npos) {
        err = "Malformed SELECT";
        return false;
    }

    std::string select_list = trim(s.substr(std::string("SELECT").size(), from_pos - std::string("SELECT").size()));
    std::string after_from = trim(s.substr(from_pos + std::string(" FROM ").size()));

    std::string where_part;
    size_t where_pos = upper(after_from).find(" WHERE ");
    if (where_pos != std::string::npos) {
        where_part = trim(after_from.substr(where_pos + std::string(" WHERE ").size()));
        after_from = trim(after_from.substr(0, where_pos));
    }

    bool has_join = false;
    std::string left_table;
    std::string right_table;
    std::string on_part;

    size_t join_pos = upper(after_from).find(" INNER JOIN ");
    if (join_pos != std::string::npos) {
        has_join = true;
        left_table = trim(after_from.substr(0, join_pos));
        std::string after_join = trim(after_from.substr(join_pos + std::string(" INNER JOIN ").size()));
        size_t on_pos = upper(after_join).find(" ON ");
        if (on_pos == std::string::npos) {
            err = "Malformed JOIN";
            return false;
        }
        right_table = trim(after_join.substr(0, on_pos));
        on_part = trim(after_join.substr(on_pos + std::string(" ON ").size()));
    } else {
        left_table = trim(after_from);
    }

    auto itL = g_db.schemas.find(upper(left_table));
    if (itL == g_db.schemas.end()) {
        err = "no such table";
        return false;
    }
    const TableSchema &L = itL->second;

    // Ensure buffered appends are visible before reading.
    flush_runtime_table_file(L.name);

    const TableSchema *R = nullptr;
    Condition join_cond;
    if (has_join) {
        auto itR = g_db.schemas.find(upper(right_table));
        if (itR == g_db.schemas.end()) {
            err = "no such table";
            return false;
        }
        R = &itR->second;

        flush_runtime_table_file(R->name);
        std::string jerr;
        if (!parse_where(on_part, join_cond, jerr)) {
            err = "Invalid JOIN condition";
            return false;
        }
    }

    std::optional<Condition> where_cond;
    if (!where_part.empty()) {
        Condition c;
        std::string werr;
        if (!parse_where(where_part, c, werr)) {
            err = werr;
            return false;
        }
        where_cond = c;
    }

    std::vector<std::string> projection;
    if (trim(select_list) == "*") {
        if (!has_join) {
            for (const auto &col : L.columns) {
                projection.push_back(col.name);
            }
        } else {
            for (const auto &col : L.columns) {
                projection.push_back(L.name + "." + col.name);
            }
            for (const auto &col : R->columns) {
                projection.push_back(R->name + "." + col.name);
            }
        }
    } else {
        projection = split_csv_top_level(select_list);
        for (auto &p : projection) {
            p = trim(p);
        }
    }

    // Non-join projection helper: resolve projection columns to indices once per query.
    std::unordered_map<std::string, size_t> col_to_idx;
    std::vector<size_t> proj_indices;
    if (!has_join) {
        col_to_idx.reserve(L.columns.size() * 2);
        for (size_t i = 0; i < L.columns.size(); i++) {
            col_to_idx[upper(L.columns[i].name)] = i;
            col_to_idx[upper(L.name + "." + L.columns[i].name)] = i;
        }
        proj_indices.reserve(projection.size());
        for (const auto &col : projection) {
            auto itc = col_to_idx.find(upper(col));
            if (itc == col_to_idx.end()) {
                err = "unknown column";
                return false;
            }
            proj_indices.push_back(itc->second);
        }
    }

    auto build_row = [&](const std::vector<std::string> &names, const std::vector<std::string> &values) -> bool {
        if (where_cond.has_value()) {
            if (!eval_condition(where_cond.value(), names, values)) {
                return true;
            }
        }

        std::vector<std::string> out_vals;
        out_vals.reserve(projection.size());
        if (!has_join) {
            for (size_t k = 0; k < proj_indices.size(); k++) {
                out_vals.push_back(values[proj_indices[k]]);
            }
        } else {
            for (const auto &col : projection) {
                bool found = false;
                for (size_t i = 0; i < names.size(); i++) {
                    if (upper(names[i]) == upper(col)) {
                        out_vals.push_back(values[i]);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    err = "unknown column";
                    return false;
                }
            }
        }
        send_row(client_socket, projection, out_vals);
        return true;
    };

    // Collect response for caching (small only).
    std::vector<std::string> cache_lines;
    size_t cache_bytes = 0;
    auto maybe_cache_line = [&](const std::string &line) {
        // Cache only small result sets.
        if (cache_bytes > g_db.cache_bytes_limit / 2) {
            return;
        }
        cache_lines.push_back(line);
        cache_bytes += line.size();
    };

    // Wrap send_row for caching.
    auto send_row_cached = [&](const std::vector<std::string> &col_names, const std::vector<std::string> &col_values) {
        if (client_socket < 0) {
            return;
        }
        std::string row = "ROW ";
        row += std::to_string((int)col_values.size());
        row += " ";
        for (size_t i = 0; i < col_values.size(); i++) {
            const std::string &name = col_names[i];
            const std::string &value = col_values[i];
            row += std::to_string(name.size());
            row += ":";
            row += name;
            row += std::to_string(value.size());
            row += ":";
            row += value;
        }
        row += "\n";
        row_outbuf += row;
        if (row_outbuf.size() >= kRowOutFlushBytes) {
            flush_row_outbuf();
        }
        maybe_cache_line(row);
    };

    auto build_row_cached = [&](const std::vector<std::string> &names, const std::vector<std::string> &values) -> bool {
        if (where_cond.has_value()) {
            if (!eval_condition(where_cond.value(), names, values)) {
                return true;
            }
        }

        std::vector<std::string> out_vals;
        out_vals.reserve(projection.size());
        if (!has_join) {
            for (size_t k = 0; k < proj_indices.size(); k++) {
                out_vals.push_back(values[proj_indices[k]]);
            }
        } else {
            for (const auto &col : projection) {
                bool found = false;
                for (size_t i = 0; i < names.size(); i++) {
                    if (upper(names[i]) == upper(col)) {
                        out_vals.push_back(values[i]);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    err = "unknown column";
                    return false;
                }
            }
        }

        send_row_cached(projection, out_vals);
        return true;
    };

    if (!has_join) {
        std::vector<std::string> names;
        names.reserve(L.columns.size());
        for (const auto &c : L.columns) {
            names.push_back(c.name);
        }

        // Fast path: WHERE on integer PK (equality or range).
        if (where_cond.has_value()) {
            ensure_runtime_for_table(L);
            auto &rt = g_db.runtime[upper(L.name)];
            std::string lhsu = upper(where_cond->lhs);
            std::string pku = upper(L.columns[rt.pk_col].name);
            // Rebuild PK index lazily if inserts have made it stale.
            if (rt.has_int_pk_index && !rt.pk_index_valid) {
                std::string rb_err;
                rebuild_pk_index_for_table(L, rb_err);
            }
            if (rt.has_int_pk_index && rt.pk_index_valid && (lhsu == pku || lhsu == upper(L.name + "." + L.columns[rt.pk_col].name))) {
                long long rhs_pk = 0;
                if (parse_int64_strict(where_cond->rhs, rhs_pk)) {
                    long long start = 1;
                    long long end = (long long)rt.pk_offsets.size() - 1;
                    if (where_cond->op == "=") {
                        start = rhs_pk;
                        end = rhs_pk;
                    } else if (where_cond->op == ">") {
                        start = rhs_pk + 1;
                    } else if (where_cond->op == ">=") {
                        start = rhs_pk;
                    } else if (where_cond->op == "<") {
                        end = rhs_pk - 1;
                    } else if (where_cond->op == "<=") {
                        end = rhs_pk;
                    }

                    start = std::max(1LL, start);
                    end = std::min(end, (long long)rt.pk_offsets.size() - 1);
                    if (start <= end) {
                        for (long long pk = start; pk <= end; pk++) {
                            uint64_t off = rt.pk_offsets[(size_t)pk];
                            if (off == 0) {
                                continue;
                            }
                            std::vector<std::string> out_vals;
                            std::optional<std::string> expires_at;
                            if (!table_read_row_at_projected(L, off, proj_indices, out_vals, expires_at, err)) {
                                return false;
                            }
                            if (expires_at.has_value()) {
                                auto n = to_number(expires_at.value());
                                if (n.has_value()) {
                                    std::time_t now = std::time(nullptr);
                                    if ((long double)now > n.value()) {
                                        continue;
                                    }
                                }
                            }
                            if (out_vals.size() != projection.size()) {
                                err = "Corrupt table file";
                                continue;
                            }
                            send_row_cached(projection, out_vals);
                        }

                        if (client_socket >= 0) {
                            if (cache_bytes <= g_db.cache_bytes_limit / 2) {
                                cache_put(sql, cache_lines, cache_bytes);
                            }
                        }
                        flush_row_outbuf();
                        return true;
                    }
                }
            }
        }

        bool ok = table_scan(L, [&](const std::vector<std::string> &row) -> bool {
            if (row_is_expired(L, row)) {
                return true;
            }

            if (client_socket >= 0) {
                return build_row_cached(names, row);
            }

            return build_row(names, row);
        }, err);

        flush_row_outbuf();

        if (ok && client_socket >= 0) {
            if (cache_bytes <= g_db.cache_bytes_limit / 2) {
                cache_put(sql, cache_lines, cache_bytes);
            }
        }
        flush_row_outbuf();
        return ok;
    }

    // Join: use right-side PK index if join RHS is that PK (fast path).
    std::vector<std::string> joined_names;
    joined_names.reserve(L.columns.size() + R->columns.size());
    for (const auto &c : L.columns) {
        joined_names.push_back(L.name + "." + c.name);
    }
    for (const auto &c : R->columns) {
        joined_names.push_back(R->name + "." + c.name);
    }

    std::unordered_map<std::string, size_t> joined_col_to_idx;
    joined_col_to_idx.reserve(joined_names.size() * 2);
    for (size_t i = 0; i < joined_names.size(); i++) {
        joined_col_to_idx[upper(joined_names[i])] = i;
    }
    std::vector<size_t> join_proj_indices;
    join_proj_indices.reserve(projection.size());
    for (const auto &col : projection) {
        auto itc = joined_col_to_idx.find(upper(col));
        if (itc == joined_col_to_idx.end()) {
            err = "unknown column";
            return false;
        }
        join_proj_indices.push_back(itc->second);
    }

    std::optional<size_t> where_join_idx = std::nullopt;
    if (where_cond.has_value()) {
        auto itw = joined_col_to_idx.find(upper(where_cond->lhs));
        if (itw == joined_col_to_idx.end()) {
            // Keep legacy semantics: unknown column in WHERE simply yields no matches.
            // (eval_condition would return false for all rows)
            where_join_idx = std::nullopt;
        } else {
            where_join_idx = itw->second;
        }
    }

    auto eval_where_value = [&](const std::string &lhs_val) -> bool {
        if (!where_cond.has_value()) {
            return true;
        }
        const Condition &cond = where_cond.value();
        if (cond.rhs_is_string) {
            const std::string &a = lhs_val;
            const std::string &b = cond.rhs;
            if (cond.op == "=") return a == b;
            if (cond.op == ">") return a > b;
            if (cond.op == "<") return a < b;
            if (cond.op == ">=") return a >= b;
            if (cond.op == "<=") return a <= b;
            return false;
        }
        auto a = to_number(lhs_val);
        auto b = to_number(cond.rhs);
        if (!a.has_value() || !b.has_value()) {
            return false;
        }
        if (cond.op == "=") return a.value() == b.value();
        if (cond.op == ">") return a.value() > b.value();
        if (cond.op == "<") return a.value() < b.value();
        if (cond.op == ">=") return a.value() >= b.value();
        if (cond.op == "<=") return a.value() <= b.value();
        return false;
    };

    auto get_join_key = [&](const TableSchema &T, const std::vector<std::string> &row, const std::string &qualified_or_unqualified) -> std::optional<std::string> {
        std::string upwant = upper(qualified_or_unqualified);
        for (size_t i = 0; i < T.columns.size(); i++) {
            std::string q = upper(T.name + "." + T.columns[i].name);
            std::string u = upper(T.columns[i].name);
            if (upwant == q || upwant == u) {
                return row[i];
            }
        }
        return std::nullopt;
    };

    ensure_runtime_for_table(*R);
    auto &rtR = g_db.runtime[upper(R->name)];
    bool rhs_is_pk = rtR.has_int_pk_index && (upper(join_cond.rhs) == upper(R->columns[rtR.pk_col].name) || upper(join_cond.rhs) == upper(R->name + "." + R->columns[rtR.pk_col].name));

    bool ok = table_scan(L, [&](const std::vector<std::string> &lr) -> bool {
        if (row_is_expired(L, lr)) {
            return true;
        }
        auto lk = get_join_key(L, lr, join_cond.lhs);
        if (!lk.has_value()) {
            err = "Invalid JOIN column";
            return false;
        }

        if (rhs_is_pk && join_cond.op == "=") {
            long long pk = 0;
            if (parse_int64_strict(lk.value(), pk) && pk > 0 && pk < (long long)rtR.pk_offsets.size()) {
                uint64_t off = rtR.pk_offsets[(size_t)pk];
                if (off != 0) {
                    std::vector<size_t> rhs_needed;
                    rhs_needed.reserve(join_proj_indices.size() + 1);
                    std::vector<int32_t> rhs_pos;
                    rhs_pos.assign(R->columns.size(), -1);

                    auto need_rhs_col = [&](size_t rhs_col_idx) {
                        if (rhs_col_idx >= rhs_pos.size()) {
                            return;
                        }
                        if (rhs_pos[rhs_col_idx] >= 0) {
                            return;
                        }
                        rhs_pos[rhs_col_idx] = (int32_t)rhs_needed.size();
                        rhs_needed.push_back(rhs_col_idx);
                    };

                    // RHS columns needed for projection
                    for (size_t i = 0; i < join_proj_indices.size(); i++) {
                        size_t jidx = join_proj_indices[i];
                        if (jidx >= L.columns.size()) {
                            need_rhs_col(jidx - L.columns.size());
                        }
                    }

                    // RHS column needed for WHERE (if any)
                    if (where_join_idx.has_value()) {
                        size_t widx = where_join_idx.value();
                        if (widx >= L.columns.size()) {
                            need_rhs_col(widx - L.columns.size());
                        }
                    }

                    std::vector<std::string> rhs_vals;
                    std::optional<std::string> rhs_expires_at;
                    std::string rerr;
                    if (!table_read_row_at_projected(*R, off, rhs_needed, rhs_vals, rhs_expires_at, rerr)) {
                        err = rerr;
                        return false;
                    }
                    if (rhs_expires_at.has_value()) {
                        auto n = to_number(rhs_expires_at.value());
                        if (n.has_value()) {
                            std::time_t now = std::time(nullptr);
                            if ((long double)now > n.value()) {
                                return true;
                            }
                        }
                    }

                    if (where_cond.has_value() && where_join_idx.has_value()) {
                        size_t widx = where_join_idx.value();
                        std::string lhs_val;
                        if (widx < L.columns.size()) {
                            lhs_val = lr[widx];
                        } else {
                            size_t rcol = widx - L.columns.size();
                            int32_t p = (rcol < rhs_pos.size()) ? rhs_pos[rcol] : -1;
                            if (p < 0 || (size_t)p >= rhs_vals.size()) {
                                return true;
                            }
                            lhs_val = rhs_vals[(size_t)p];
                        }
                        if (!eval_where_value(lhs_val)) {
                            return true;
                        }
                    } else if (where_cond.has_value() && !where_join_idx.has_value()) {
                        // unknown WHERE column => no rows match
                        return true;
                    }

                    if (client_socket >= 0) {
                        std::vector<std::string> out_vals;
                        out_vals.reserve(projection.size());
                        for (size_t i = 0; i < join_proj_indices.size(); i++) {
                            size_t jidx = join_proj_indices[i];
                            if (jidx < L.columns.size()) {
                                out_vals.push_back(lr[jidx]);
                            } else {
                                size_t rcol = jidx - L.columns.size();
                                int32_t p = (rcol < rhs_pos.size()) ? rhs_pos[rcol] : -1;
                                if (p < 0 || (size_t)p >= rhs_vals.size()) {
                                    err = "Corrupt table file";
                                    return false;
                                }
                                out_vals.push_back(rhs_vals[(size_t)p]);
                            }
                        }
                        send_row_cached(projection, out_vals);
                    } else {
                        // Keep server-side (non-client) path simple.
                        std::vector<std::string> rr;
                        if (!table_read_row_at(*R, off, rr, rerr)) {
                            err = rerr;
                            return false;
                        }
                        if (!row_is_expired(*R, rr)) {
                            std::vector<std::string> joined_vals;
                            joined_vals.reserve(lr.size() + rr.size());
                            joined_vals.insert(joined_vals.end(), lr.begin(), lr.end());
                            joined_vals.insert(joined_vals.end(), rr.begin(), rr.end());
                            if (!build_row(joined_names, joined_vals)) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }

        std::string inner_err;
        bool inner_ok = table_scan(*R, [&](const std::vector<std::string> &rr) -> bool {
            if (row_is_expired(*R, rr)) {
                return true;
            }
            auto rk = get_join_key(*R, rr, join_cond.rhs);
            if (!rk.has_value()) {
                inner_err = "Invalid JOIN column";
                return false;
            }
            if (!compare_values(lk.value(), rk.value(), join_cond.op)) {
                return true;
            }
            std::vector<std::string> joined_vals;
            joined_vals.reserve(lr.size() + rr.size());
            joined_vals.insert(joined_vals.end(), lr.begin(), lr.end());
            joined_vals.insert(joined_vals.end(), rr.begin(), rr.end());
            if (!build_row(joined_names, joined_vals)) {
                return false;
            }
            return true;
        }, inner_err);

        if (!inner_ok) {
            err = inner_err;
            return false;
        }
        return true;
    }, err);

    if (ok && client_socket >= 0) {
        if (cache_bytes <= g_db.cache_bytes_limit / 2) {
            cache_put(sql, cache_lines, cache_bytes);
        }
    }

    flush_row_outbuf();
    return ok;
}

static bool execute_sql(const std::string &sql_in, int client_socket, std::string &err) {
    std::string s = trim(sql_in);
    if (!s.empty() && s.back() == ';') {
        s.pop_back();
    }
    s = trim(s);
    if (s.empty()) {
        err = "Empty SQL";
        return false;
    }

    std::string up = upper(s);
    if (starts_with_ci(up, "CREATE TABLE")) {
        return exec_create_table(s, err);
    }
    if (starts_with_ci(up, "INSERT INTO")) {
        return exec_insert(s, err);
    }
    if (starts_with_ci(up, "SELECT")) {
        return exec_select(s, client_socket, err);
    }

    err = "Unsupported SQL";
    return false;
}

static bool replay_wal(std::string &err) {
    std::ifstream in(WAL_PATH, std::ios::binary);
    if (!in.is_open()) {
        return true;
    }

    g_db.replaying = true;

    uint64_t last_good_offset = 0;

    while (true) {
        last_good_offset = (uint64_t)in.tellg();
        uint32_t len = 0;
        in.read(reinterpret_cast<char *>(&len), sizeof(len));
        if (!in) {
            break;
        }
        if (len == 0 || len > (32u * 1024u * 1024u)) {
            // tolerate truncated/corrupt tail
            err.clear();
            break;
        }
        std::string sql;
        sql.resize(len);
        in.read(&sql[0], (std::streamsize)len);
        if (!in) {
            err.clear();
            break;
        }
        uint32_t crc = 0;
        in.read(reinterpret_cast<char *>(&crc), sizeof(crc));
        if (!in) {
            err.clear();
            break;
        }
        uint32_t got = crc32(reinterpret_cast<const uint8_t *>(sql.data()), sql.size());
        if (got != crc) {
            // tolerate corrupt tail
            err.clear();
            break;
        }

        std::string exec_err;
        (void)execute_sql(sql, -1, exec_err);
    }

    g_db.replaying = false;
    // Flush any arenas accumulated during replay before clearing cache.
    flush_all_pending();
    cache_clear();

    // If we stopped early due to a corrupt/torn tail record, truncate it away.
    // This avoids failing future replays.
    if (!in.bad()) {
        (void)::truncate(WAL_PATH, (off_t)last_good_offset);
    }
    return err.empty();
}

static void handle_client(int client_socket) {
    std::string pending;  // holds partial tail between pipeline chunks
    std::string outbuf;
    const size_t kOutbufFlushBytes = 16u * 1024u * 1024u;  // 16MB before flushing responses

    bool bulk_ack_mode = false;
    size_t bulk_ack_pending = 0;

    std::string last_tkey;
    TableSchema *last_schema = nullptr;
    Database::TableRuntime *last_rt = nullptr;
    std::mutex *last_table_mu = nullptr;
    std::string *last_arena = nullptr;   // &write_arenas[last_tkey]
    size_t last_values_offset = 0;       // pre-computed offset past '(' in INSERT INTO tbl VALUES (
    size_t last_ncols = 0;               // last_schema->columns.size() cached
    // Per-connection local arena: parse into this, flush to shared writer under brief lock.
    // Eliminates per-row spinlock contention when multiple threads insert to same table.
    std::string local_arena;
    local_arena.reserve(Database::WRITE_ARENA_FLUSH + 4096);

    std::string table_name_tmp;
    std::vector<std::string_view> single_vals;
    std::string sv_err;
    std::string wal_sql_tmp;

    // Batch spinlock: hold the table spinlock across consecutive inserts
    // to the same table within one recv() burst, instead of lock/unlock per row.
    // Released whenever we switch tables, hit a non-INSERT, or flush acks.
    Database::TableRuntime *spin_held_rt = nullptr;

    auto release_batch_spin = [&]() {
        if (spin_held_rt) {
            spin_held_rt->spin.clear(std::memory_order_release);
            spin_held_rt = nullptr;
        }
    };
    auto acquire_batch_spin = [&](Database::TableRuntime *rt) {
        if (spin_held_rt == rt) return;  // already held for this table
        release_batch_spin();
        int s = 0;
        while (rt->spin.test_and_set(std::memory_order_acquire)) {
            if (++s > 1000) { std::this_thread::yield(); s = 0; }
        }
        spin_held_rt = rt;
    };

    // Per-connection WAL buffer (only used when WAL is enabled).
    std::string conn_wal_buf;
    size_t conn_wal_count = 0;
    static constexpr size_t CONN_WAL_FLUSH = 8192;
    if (g_db.wal_enabled) conn_wal_buf.reserve(1u << 20);

    // Flush conn_wal_buf into g_db.wal_batch_buf under g_db.mu.
    auto flush_conn_wal = [&]() {
        if (!g_db.wal_enabled || conn_wal_buf.empty()) {
            conn_wal_buf.clear();
            conn_wal_count = 0;
            return;
        }
        std::lock_guard<std::mutex> lk(g_db.mu);
        const size_t flushed_count = conn_wal_count;
        g_db.wal_batch_buf.append(conn_wal_buf);
        conn_wal_buf.clear();
        conn_wal_count = 0;
        g_db.wal_batch_count += flushed_count;
        if (g_db.wal_batch_buf.size() >= Database::WAL_BATCH_BYTES ||
            g_db.wal_batch_count >= Database::WAL_BATCH_COUNT) {
            wal_flush_batch();
        }
    };

    // Recv loop: 4MB thread_local buffer + greedy drain handles large batch INSERTs.
    // 4MB buffer ensures batch INSERT strings (up to ~4MB each) fit in one recv.
    static constexpr size_t RECV_BUF_SIZE = 4u * 1024u * 1024u;
    static thread_local char recv_buf[RECV_BUF_SIZE];
    while (true) {
        ssize_t bytes_read = ::read(client_socket, recv_buf, RECV_BUF_SIZE);
        if (bytes_read <= 0) break;
        size_t raw_size = (size_t)bytes_read;
        // Greedy drain: fill buffer with all available data
        while (raw_size < RECV_BUF_SIZE) {
            ssize_t n2 = ::recv(client_socket, recv_buf + raw_size,
                                RECV_BUF_SIZE - raw_size, MSG_DONTWAIT);
            if (n2 <= 0) break;
            raw_size += (size_t)n2;
        }
        const char *raw_data = recv_buf;

        // Prepend partial tail from previous recv if any.
        const char *proc_data;
        size_t proc_size;
        std::string combined;
        if (!pending.empty()) {
            combined.reserve(pending.size() + raw_size);
            combined.append(pending);
            combined.append(raw_data, raw_size);
            proc_data = combined.data();
            proc_size = combined.size();
            pending.clear();
        } else {
            proc_data = raw_data;
            proc_size = raw_size;
        }

        size_t start = 0;
        while (start < proc_size) {
            // memchr is SIMD-accelerated: find next statement terminator.
            const char *sc = (const char *)std::memchr(proc_data + start, ';', proc_size - start);
            if (!sc) {
                // Partial statement at end of buffer — save the tail.
                pending.assign(proc_data + start, proc_size - start);
                break;
            }
            size_t pos = (size_t)(sc - proc_data);
            std::string_view sv_sql(proc_data + start, (pos - start) + 1);
            start = pos + 1;

            // ---- ULTRA-FAST BULK INSERT PATH ----
            // When in bulk_ack_mode with a known table, skip ALL dispatch overhead.
            // This is the critical hot path for 10M row benchmarks.
            if (bulk_ack_mode && last_schema && last_rt &&
                sv_sql.size() > 12 && (sv_sql[0]=='I'||sv_sql[0]=='i') &&
                sv_sql[6]==' ') {  // 'INSERT ' verified
                std::string_view sv_no_semi2 = sv_sql;
                if (!sv_no_semi2.empty() && sv_no_semi2.back()==';') sv_no_semi2.remove_suffix(1);
                while (!sv_no_semi2.empty() && (unsigned char)sv_no_semi2.back()<=' ') sv_no_semi2.remove_suffix(1);
                // SINGLE-PASS: cached arena + pre-computed offset, no double-parse.
                if (__builtin_expect(last_arena != nullptr, 1)) {
                    std::string &arena2 = *last_arena;
                    if (__builtin_expect(arena2.capacity() < Database::WRITE_ARENA_FLUSH, 0))
                        arena2.reserve(Database::WRITE_ARENA_FLUSH + 4096);
                    acquire_batch_spin(last_rt);
                    size_t written2 = parse_insert_direct_to_arena(
                        sv_no_semi2, last_ncols, arena2, last_values_offset);
                    if (__builtin_expect(written2 > 0, 1)) {
                        last_rt->data_end_offset += written2;
                        last_rt->data_dirty = true;
                        if (__builtin_expect(arena2.size() >= Database::WRITE_ARENA_FLUSH, 0))
                            flush_arena_to_async_writer(last_tkey, arena2, last_rt->data_fp);
                        cache_invalidate();
                        // Append WAL for ultra-fast inserts as well.
                        if (g_db.wal_enabled) {
                            wal_sql_tmp.assign(sv_no_semi2.data(), sv_no_semi2.size());
                            wal_sql_tmp.push_back(';');
                            uint32_t wlen = (uint32_t)wal_sql_tmp.size();
                            uint32_t wcrc = crc32(reinterpret_cast<const uint8_t *>(wal_sql_tmp.data()), wal_sql_tmp.size());
                            conn_wal_buf.append(reinterpret_cast<const char *>(&wlen), sizeof(wlen));
                            conn_wal_buf.append(wal_sql_tmp.data(), wal_sql_tmp.size());
                            conn_wal_buf.append(reinterpret_cast<const char *>(&wcrc), sizeof(wcrc));
                            conn_wal_count++;
                            if (conn_wal_count >= CONN_WAL_FLUSH || conn_wal_buf.size() >= 2u * 1024u * 1024u) {
                                flush_conn_wal();
                            }
                        }
                        bulk_ack_pending++;
                        continue;
                    }
                    release_batch_spin();
                }
            }
            // ---- END ULTRA-FAST PATH ----

            std::string err;
            bool ok = false;
            std::string_view sv = sv_sql;
            while (!sv.empty() && (unsigned char)sv.front() <= ' ') sv.remove_prefix(1);
            while (!sv.empty() && (unsigned char)sv.back()  <= ' ') sv.remove_suffix(1);
            auto get_sql_str = [&]() -> std::string { return std::string(sv_sql); };
            (void)get_sql_str;

            auto flush_bulk_acks = [&]() {
                if (bulk_ack_mode && bulk_ack_pending > 0) {
                    outbuf += "OKB ";
                    outbuf += std::to_string(bulk_ack_pending);
                    outbuf += "\nEND\n";
                    bulk_ack_pending = 0;
                }
            };

            if (starts_with_ci(sv, "FLEXQL_ACKMODE")) {
                release_batch_spin();
                flush_bulk_acks();
                flush_conn_wal();
                std::string_view p = sv;
                if (!p.empty() && p.back() == ';') {
                    p = p.substr(0, p.size() - 1);
                    p = trim_sv(p);
                }
                // Accept: FLEXQL_ACKMODE BULK
                std::string_view rest = p.substr(std::string_view("FLEXQL_ACKMODE").size());
                rest = trim_sv(rest);
                if (starts_with_ci(rest, "BULK")) {
                    bulk_ack_mode = true;
                    ok = true;
                } else {
                    err = "unknown ack mode";
                    ok = false;
                }
            } else if (sv.size() >= 11 &&
                         (sv[0]=='I'||sv[0]=='i') &&
                         (sv[1]=='N'||sv[1]=='n') &&
                         (sv[2]=='S'||sv[2]=='s') &&
                         (sv[6]==' ') &&
                         (sv[7]=='I'||sv[7]=='i') &&
                         (sv[8]=='N'||sv[8]=='n') &&
                         (sv[9]=='T'||sv[9]=='t') &&
                         (sv[10]=='O'||sv[10]=='o')) {
                std::string_view sv_no_semi = sv;
                if (!sv_no_semi.empty() && sv_no_semi.back() == ';') {
                    sv_no_semi = sv_no_semi.substr(0, sv_no_semi.size() - 1);
                    sv_no_semi = trim_sv(sv_no_semi);
                }

                std::string_view table_sv;
                bool single_view_ok = fast_parse_insert_single_row_views_sv(sv_no_semi, table_sv, single_vals, sv_err);
                if (single_view_ok) {
                    // Fast path: if table_sv (case-insensitive) matches last_tkey, skip upper().
                    bool tkey_changed = (table_sv.size() != last_tkey.size());
                    if (!tkey_changed && !last_tkey.empty()) {
                        for (size_t _k = 0; _k < table_sv.size() && !tkey_changed; _k++) {
                            char a = table_sv[_k]; if (a>='a'&&a<='z') a-=32;
                            if (a != last_tkey[_k]) tkey_changed = true;
                        }
                    } else if (last_tkey.empty()) { tkey_changed = true; }
                    if (tkey_changed || !last_schema || !last_rt || !last_table_mu) {
                        table_name_tmp.assign(table_sv);
                        std::string tkey = upper(table_name_tmp);
                        release_batch_spin();  // switching tables: release old lock first
                        {
                            std::lock_guard<std::mutex> lk(g_db.mu);
                            auto it = g_db.schemas.find(tkey);
                            if (it == g_db.schemas.end()) {
                                err = "Table does not exist";
                                ok = false;
                            } else {
                                last_tkey = tkey;
                                last_schema = &it->second;
                                ensure_runtime_for_table(*last_schema);
                                last_rt = &g_db.runtime[tkey];
                                last_table_mu = &get_table_mutex_locked(tkey);
                                last_arena = &g_db.write_arenas[tkey];
                                if (last_arena->capacity() < Database::WRITE_ARENA_FLUSH)
                                    last_arena->reserve(Database::WRITE_ARENA_FLUSH + 4096);
                                last_ncols = last_schema->columns.size();
                                // "INSERT INTO "(12) + table + " VALUES ("(9) -> offset past '('
                                last_values_offset = 12 + last_tkey.size() + 9;
                             }
                        }
                    }
                    if (!last_schema || !last_rt || !last_table_mu) {
                        if (err.empty()) {
                            err = "Insert fast path failed";
                        }
                        ok = false;
                    } else {
                        if (single_vals.size() != last_schema->columns.size()) {
                            err = "Column count mismatch";
                            ok = false;
                        } else {
                            {
                                // Acquire batch spinlock once; held across consecutive
                                // inserts to same table — eliminates per-row mutex cost.
                                acquire_batch_spin(last_rt);
                                if (!validate_insert_row_values_sv(*last_schema, single_vals, err)) {
                                    ok = false;
                                } else {
                                    ok = table_append_row_views_rt(*last_schema, *last_rt, single_vals, err, last_tkey);
                                }
                            }
                            if (ok) {
                                // cache_invalidate is lock-free (atomic store).
                                cache_invalidate();
                                // Accumulate WAL entry in per-connection buffer.
                                if (g_db.wal_enabled) {
                                    wal_sql_tmp.assign(sv_no_semi.data(), sv_no_semi.size());
                                    wal_sql_tmp.push_back(';');
                                    uint32_t wlen = (uint32_t)wal_sql_tmp.size();
                                    uint32_t wcrc = crc32(reinterpret_cast<const uint8_t *>(wal_sql_tmp.data()), wal_sql_tmp.size());
                                    conn_wal_buf.append(reinterpret_cast<const char *>(&wlen), sizeof(wlen));
                                    conn_wal_buf.append(wal_sql_tmp.data(), wal_sql_tmp.size());
                                    conn_wal_buf.append(reinterpret_cast<const char *>(&wcrc), sizeof(wcrc));
                                    conn_wal_count++;
                                    if (conn_wal_count >= CONN_WAL_FLUSH || conn_wal_buf.size() >= 2u * 1024u * 1024u) {
                                        flush_conn_wal();
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // exec_insert handles trailing ';' and other normalization internally.
                    ok = exec_insert(std::string(sv), err);
                }
            } else {
                release_batch_spin();  // must release before taking g_db.mu
                flush_bulk_acks();
                flush_conn_wal();  // flush WAL before any SELECT/DDL
                std::lock_guard<std::mutex> lk(g_db.mu);
                ok = execute_sql(std::string(sv_sql), client_socket, err);
            }

            if (!ok) {
                flush_bulk_acks();
                outbuf += std::string("ERROR: ") + (err.empty() ? "unknown error" : err) + "\nEND\n";
            } else {
                if (bulk_ack_mode && starts_with_ci(sv, "INSERT INTO")) {
                    bulk_ack_pending++;
                    // Avoid unbounded buffering if client never drains.
                    if (bulk_ack_pending >= 4096) {
                        flush_bulk_acks();
                    }
                } else {
                    outbuf += "END\n";
                }
            }

            if (outbuf.size() >= kOutbufFlushBytes) {
                send_text(client_socket, outbuf);
                outbuf.clear();
            }
        }

        // Save any unprocessed partial statement tail for next chunk.
        if (start < proc_size) {
            pending.assign(proc_data + start, proc_size - start);
        }

        // Only flush outbuf when it exceeds threshold to minimise send() syscalls.
        if (outbuf.size() >= kOutbufFlushBytes) {
            if (bulk_ack_mode && bulk_ack_pending > 0) {
                outbuf += "OKB ";
                outbuf += std::to_string(bulk_ack_pending);
                outbuf += "\nEND\n";
                bulk_ack_pending = 0;
            }
            send_text(client_socket, outbuf);
            outbuf.clear();
        }

        // Flush any accumulated responses for this receive batch.
        // This avoids deadlock with synchronous clients that send one statement
        // and then block waiting for END before sending more bytes.
        if (bulk_ack_mode && bulk_ack_pending > 0) {
            outbuf += "OKB ";
            outbuf += std::to_string(bulk_ack_pending);
            outbuf += "\nEND\n";
            bulk_ack_pending = 0;
        }
        if (!outbuf.empty()) {
            send_text(client_socket, outbuf);
            outbuf.clear();
        }
    }

    if (bulk_ack_mode && bulk_ack_pending > 0) {
        outbuf += "OKB ";
        outbuf += std::to_string(bulk_ack_pending);
        outbuf += "\nEND\n";
        bulk_ack_pending = 0;
    }
    if (!outbuf.empty()) {
        send_text(client_socket, outbuf);
        outbuf.clear();
    }

    // Flush connection WAL buffer then all arenas on disconnect.
    release_batch_spin();
    flush_conn_wal();
    {
        std::lock_guard<std::mutex> lk(g_db.mu);
        flush_all_pending();
    }

    ::close(client_socket);
}

} // namespace flexql

int main() {
    using namespace flexql;

    (void)::remove("flexql.db");
    {
        std::ofstream f("flexql.db", std::ios::binary | std::ios::out | std::ios::trunc);
    }

    ensure_data_dir();

    wipe_data_dir_best_effort();

    const char *enable_wal = std::getenv("FLEXQL_ENABLE_WAL");
    if (enable_wal && std::string(enable_wal) != "0") {
        g_db.wal_enabled = true;
    }
    const char *no_wal = std::getenv("FLEXQL_DISABLE_WAL");
    if (no_wal && std::string(no_wal) != "0") {
        g_db.wal_enabled = false;
    }

    std::string err;
    if (!catalog_load(err)) {
        std::cerr << err << "\n";
        return 1;
    }
    if (!replay_wal(err)) {
        std::cerr << err << "\n";
        return 1;
    }

    for (const auto &kv : g_db.schemas) {
        std::string ierr;
        if (!rebuild_pk_index_for_table(kv.second, ierr)) {
            std::cerr << (ierr.empty() ? "Index rebuild failed" : ierr) << "\n";
            return 1;
        }
    }

    if (g_db.wal_enabled) {
        g_db.wal.open(WAL_PATH, std::ios::binary | std::ios::out | std::ios::app);
        if (!g_db.wal.is_open()) {
            std::cerr << "Failed to open WAL" << std::endl;
            return 1;
        }
    }

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket failed\n";
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cerr << "bind failed\n";
        return 1;
    }

    if (listen(server_fd, 128) < 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "FlexQL Server running on port " << PORT << std::endl;

    start_async_writer();

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_socket = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_socket < 0) {
            continue;
        }
        {
            int one = 1;
            (void)setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            // Large socket buffers: reduce recv() syscall frequency.
            int sndbuf = 4 * 1024 * 1024;
            int rcvbuf = 4 * 1024 * 1024;
            (void)setsockopt(client_socket, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
            (void)setsockopt(client_socket, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        }
        std::thread t(handle_client, client_socket);
        t.detach();
    }

    return 0;
}
