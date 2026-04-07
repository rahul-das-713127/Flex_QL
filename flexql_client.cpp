#include "flexql.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

struct FlexQL {
    int sock = -1;
    std::string pending;
    size_t pending_pos = 0;
    size_t pipelined = 0;
    std::string outbuf;
    bool bulk_ack = false;
};

static bool enable_bulk_ack(FlexQL *db);

static void set_err(char **errmsg, const std::string &msg) {
    if (!errmsg) {
        return;
    }
    char *buf = (char*)std::malloc(msg.size() + 1);
    if (!buf) {
        *errmsg = nullptr;
        return;
    }
    std::memcpy(buf, msg.c_str(), msg.size());
    buf[msg.size()] = '\0';
    *errmsg = buf;
}

void flexql_free(void *ptr) {
    std::free(ptr);
}

static int connect_tcp(const char *host, int port, char **errmsg) {
    if (!host || port <= 0 || port > 65535) {
        set_err(errmsg, "Invalid host/port");
        return -1;
    }

    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res = nullptr;
    std::string port_str = std::to_string(port);
    int rc = getaddrinfo(host, port_str.c_str(), &hints, &res);
    if (rc != 0) {
        set_err(errmsg, std::string("getaddrinfo failed: ") + gai_strerror(rc));
        return -1;
    }

    int sock = -1;
    for (struct addrinfo *p = res; p != nullptr; p = p->ai_next) {
        sock = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sock < 0) {
            continue;
        }
        {
            int one = 1;
            (void)::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        }
        if (::connect(sock, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        ::close(sock);
        sock = -1;
    }

    freeaddrinfo(res);

    if (sock < 0) {
        set_err(errmsg, std::string("connect failed: ") + std::strerror(errno));
        return -1;
    }

    return sock;
}

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!db) {
        return FLEXQL_ERROR;
    }
    *db = nullptr;

    char *err = nullptr;
    int sock = connect_tcp(host, port, &err);
    if (sock < 0) {
        if (err) {
            flexql_free(err);
        }
        return FLEXQL_ERROR;
    }

    FlexQL *h = new (std::nothrow) FlexQL();
    if (!h) {
        ::close(sock);
        return FLEXQL_ERROR;
    }
    h->sock = sock;
    (void)enable_bulk_ack(h);
    *db = h;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) {
        return FLEXQL_ERROR;
    }
    if (db->sock >= 0) {
        ::close(db->sock);
        db->sock = -1;
    }
    delete db;
    return FLEXQL_OK;
}

static bool send_all(int sock, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(sock, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        sent += (size_t)n;
    }
    return true;
}

static bool recv_line(int sock, std::string &out_line, std::string &pending, size_t &pending_pos) {
    while (true) {
        size_t pos = pending.find('\n', pending_pos);
        if (pos != std::string::npos) {
            out_line.assign(pending.data() + pending_pos, pos - pending_pos);
            pending_pos = pos + 1;

            // Compact occasionally to avoid unbounded growth.
            if (pending_pos > (1u << 20) || pending_pos > pending.size() / 2) {
                pending.erase(0, pending_pos);
                pending_pos = 0;
            }
            return true;
        }

        char buf[64 * 1024];
        ssize_t n = ::recv(sock, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        pending.append(buf, (size_t)n);
    }
}

static bool enable_bulk_ack(FlexQL *db) {
    if (!db || db->sock < 0) {
        return false;
    }
    const char *env = std::getenv("FLEXQL_BULK_ACK_INSERTS");
    if (env && env[0] == '0') {
        return false;
    }

    const char *stmt = "FLEXQL_ACKMODE BULK;";
    if (!send_all(db->sock, stmt, std::strlen(stmt))) {
        return false;
    }

    // Read response; treat errors as "feature unsupported" and continue.
    while (true) {
        std::string line;
        if (!recv_line(db->sock, line, db->pending, db->pending_pos)) {
            return false;
        }
        if (line == "END") {
            db->bulk_ack = true;
            return true;
        }
        if (line.rfind("ERROR:", 0) == 0) {
            // Consume until END.
            while (true) {
                std::string l2;
                if (!recv_line(db->sock, l2, db->pending, db->pending_pos)) {
                    return false;
                }
                if (l2 == "END") {
                    break;
                }
            }
            db->bulk_ack = false;
            return false;
        }
    }
}

static bool is_insert_sql(const char *sql) {
    if (!sql) return false;
    // Skip leading whitespace.
    const char *p = sql;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {
        ++p;
    }
    // Case-insensitive "INSERT" check.
    const char *kw = "INSERT";
    for (int i = 0; kw[i]; i++) {
        char c = p[i];
        if (c == '\0') return false;
        if (c >= 'a' && c <= 'z') c = (char)(c - 'a' + 'A');
        if (c != kw[i]) return false;
    }
    return true;
}

static bool drain_one_response(FlexQL *db, char **errmsg) {
    while (true) {
        std::string line;
        if (!recv_line(db->sock, line, db->pending, db->pending_pos)) {
            set_err(errmsg, "Connection closed while reading response");
            return false;
        }
        if (line == "END") {
            return true;
        }
        if (line.rfind("ERROR:", 0) == 0) {
            std::string msg = line.substr(6);
            while (!msg.empty() && msg[0] == ' ') {
                msg.erase(0, 1);
            }
            set_err(errmsg, msg.empty() ? "unknown error" : msg);
            // consume until END
            while (true) {
                std::string l2;
                if (!recv_line(db->sock, l2, db->pending, db->pending_pos)) {
                    break;
                }
                if (l2 == "END") {
                    break;
                }
            }
            return false;
        }
        // Ignore any other informational lines.
    }
}

static bool drain_bulk_responses(FlexQL *db, size_t n, char **errmsg) {
    size_t acked = 0;
    size_t pending_ack = 0;
    while (acked < n) {
        std::string line;
        if (!recv_line(db->sock, line, db->pending, db->pending_pos)) {
            set_err(errmsg, "Connection closed while reading response");
            return false;
        }
        if (line == "END") {
            if (pending_ack == 0) {
                // Defensive: END without OK/OKB, count as one.
                pending_ack = 1;
            }
            acked += pending_ack;
            pending_ack = 0;
            continue;
        }
        if (line == "OK") {
            pending_ack += 1;
            continue;
        }
        if (line.rfind("OKB ", 0) == 0) {
            const char *p = line.c_str() + 4;
            long long v = 0;
            while (*p >= '0' && *p <= '9') {
                v = v * 10 + (*p - '0');
                ++p;
            }
            if (v > 0) {
                pending_ack += (size_t)v;
            }
            continue;
        }
        if (line.rfind("ERROR:", 0) == 0) {
            std::string msg = line.substr(6);
            while (!msg.empty() && msg[0] == ' ') {
                msg.erase(0, 1);
            }
            set_err(errmsg, msg.empty() ? "unknown error" : msg);
            while (true) {
                std::string l2;
                if (!recv_line(db->sock, l2, db->pending, db->pending_pos)) {
                    break;
                }
                if (l2 == "END") {
                    break;
                }
            }
            return false;
        }
        // Ignore ROW/other lines (shouldn't happen for INSERT), keep scanning.
    }
    return true;
}

static bool drain_responses(FlexQL *db, size_t n, char **errmsg) {
    if (db && db->bulk_ack) {
        return drain_bulk_responses(db, n, errmsg);
    }
    for (size_t i = 0; i < n; i++) {
        if (!drain_one_response(db, errmsg)) {
            return false;
        }
    }
    return true;
}

static bool parse_row(const std::string &line, std::vector<std::string> &col_names, std::vector<std::string> &col_values, std::string &err) {
    if (line.rfind("ROW ", 0) != 0) {
        err = "Invalid ROW prefix";
        return false;
    }

    size_t i = 4;
    auto read_int = [&](long long &out) -> bool {
        if (i >= line.size()) {
            return false;
        }
        bool neg = false;
        if (line[i] == '-') {
            neg = true;
            i++;
        }
        if (i >= line.size() || line[i] < '0' || line[i] > '9') {
            return false;
        }
        long long v = 0;
        while (i < line.size() && line[i] >= '0' && line[i] <= '9') {
            v = v * 10 + (line[i] - '0');
            i++;
        }
        out = neg ? -v : v;
        return true;
    };

    long long argc_ll = 0;
    if (!read_int(argc_ll) || argc_ll < 0) {
        err = "Invalid argc";
        return false;
    }
    int argc = (int)argc_ll;

    if (i >= line.size() || line[i] != ' ') {
        err = "Malformed ROW header";
        return false;
    }
    i++;

    col_names.clear();
    col_values.clear();
    col_names.reserve(argc);
    col_values.reserve(argc);

    for (int c = 0; c < argc; c++) {
        long long name_len_ll = 0;
        if (!read_int(name_len_ll) || name_len_ll < 0) {
            err = "Invalid name length";
            return false;
        }
        if (i >= line.size() || line[i] != ':') {
            err = "Missing name separator";
            return false;
        }
        i++;
        size_t name_len = (size_t)name_len_ll;
        if (i + name_len > line.size()) {
            err = "Name length out of range";
            return false;
        }
        std::string name = line.substr(i, name_len);
        i += name_len;

        long long val_len_ll = 0;
        if (!read_int(val_len_ll) || val_len_ll < 0) {
            err = "Invalid value length";
            return false;
        }
        if (i >= line.size() || line[i] != ':') {
            err = "Missing value separator";
            return false;
        }
        i++;
        size_t val_len = (size_t)val_len_ll;
        if (i + val_len > line.size()) {
            err = "Value length out of range";
            return false;
        }
        std::string value = line.substr(i, val_len);
        i += val_len;

        col_names.push_back(name);
        col_values.push_back(value);
    }

    if (i != line.size()) {
        err = "Extra data after ROW";
        return false;
    }

    return true;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg) {

    if (errmsg) {
        *errmsg = nullptr;
    }

    if (!db || db->sock < 0 || !sql) {
        set_err(errmsg, "Invalid database handle or SQL");
        return FLEXQL_ERROR;
    }

    size_t sql_len = std::strlen(sql);
    if (sql_len == 0) {
        set_err(errmsg, "Empty SQL");
        return FLEXQL_ERROR;
    }

    bool pipeline = false;
    {
        const char *env = std::getenv("FLEXQL_PIPELINE_INSERTS");
        pipeline = !(env && env[0] == '0');
    }

    bool is_insert = is_insert_sql(sql);

    // When pipelining inserts, we may buffer outgoing bytes to reduce send() syscalls.
    // Any non-INSERT or callback query must observe that all prior INSERTs have been
    // sent and their responses drained, otherwise protocol ordering can break.
    auto flush_outbuf = [&]() -> bool {
        if (db->outbuf.empty()) {
            return true;
        }
        if (!send_all(db->sock, db->outbuf.data(), db->outbuf.size())) {
            set_err(errmsg, std::string("send failed: ") + std::strerror(errno));
            return false;
        }
        db->outbuf.clear();
        return true;
    };

    // If pipelining is enabled, keep protocol correctness by draining any pending
    // responses BEFORE sending a query (callback) or any non-INSERT statement.
    // Draining after sending would risk consuming/discarding ROW lines from the
    // new statement.
    if (pipeline && db->pipelined > 0) {
        if (callback || !is_insert) {
            if (!flush_outbuf()) {
                return FLEXQL_ERROR;
            }
            if (!drain_responses(db, db->pipelined, errmsg)) {
                db->pipelined = 0;
                return FLEXQL_ERROR;
            }
            db->pipelined = 0;
        }
    }

    // Pipelined INSERT fast path: buffer inserts and send in large chunks.
    if (pipeline && !callback && is_insert) {
        // Ensure each statement is newline-terminated to match typical usage,
        // but preserve caller bytes exactly otherwise.
        db->outbuf.append(sql, sql_len);
        db->pipelined++;

        const size_t kBatch = 2000000;  // drain every 2M inserts to minimise stalls
        const size_t kFlushBytes = 4u * 1024u * 1024u; // reduce syscalls, keep memory bounded
        if (db->pipelined >= kBatch || db->outbuf.size() >= kFlushBytes) {
            if (!flush_outbuf()) {
                db->pipelined = 0;
                return FLEXQL_ERROR;
            }
        }

        // Drain after kBatch (same batching as before) to keep the server from
        // buffering unbounded responses.
        if (db->pipelined >= kBatch) {
            if (!drain_responses(db, db->pipelined, errmsg)) {
                db->pipelined = 0;
                return FLEXQL_ERROR;
            }
            db->pipelined = 0;
        }
        return FLEXQL_OK;
    }

    if (!flush_outbuf()) {
        return FLEXQL_ERROR;
    }

    if (!send_all(db->sock, sql, sql_len)) {
        set_err(errmsg, std::string("send failed: ") + std::strerror(errno));
        return FLEXQL_ERROR;
    }

    while (true) {
        std::string line;
        if (!recv_line(db->sock, line, db->pending, db->pending_pos)) {
            set_err(errmsg, "Connection closed while reading response");
            return FLEXQL_ERROR;
        }

        if (line == "END") {
            return FLEXQL_OK;
        }

        if (line.rfind("ERROR:", 0) == 0) {
            std::string msg = line.substr(6);
            while (!msg.empty() && msg[0] == ' ') {
                msg.erase(0, 1);
            }
            set_err(errmsg, msg.empty() ? "unknown error" : msg);
            while (true) {
                std::string l2;
                if (!recv_line(db->sock, l2, db->pending, db->pending_pos)) {
                    return FLEXQL_ERROR;
                }
                if (l2 == "END") {
                    break;
                }
            }
            return FLEXQL_ERROR;
        }

        if (line == "OK") {
            continue;
        }

        if (line.rfind("OKB ", 0) == 0) {
            continue;
        }

        if (line.rfind("ROW ", 0) == 0) {
            if (!callback) {
                continue;
            }
            std::vector<std::string> names;
            std::vector<std::string> values;
            std::string perr;
            if (!parse_row(line, names, values, perr)) {
                set_err(errmsg, perr);
                return FLEXQL_ERROR;
            }

            std::vector<char*> argv;
            std::vector<char*> azCol;
            argv.reserve(values.size());
            azCol.reserve(names.size());
            for (size_t k = 0; k < values.size(); k++) {
                argv.push_back(const_cast<char*>(values[k].c_str()));
                azCol.push_back(const_cast<char*>(names[k].c_str()));
            }

            int cb_rc = callback(arg, (int)argv.size(), argv.data(), azCol.data());
            if (cb_rc != 0) {
                while (true) {
                    std::string l2;
                    if (!recv_line(db->sock, l2, db->pending, db->pending_pos)) {
                        break;
                    }
                    if (l2 == "END") {
                        break;
                    }
                }
                return FLEXQL_OK;
            }
            continue;
        }
    }
}
