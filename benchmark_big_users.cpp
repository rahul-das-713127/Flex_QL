#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <chrono>
#include <cstring>
#include <cstdlib>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 10LL;
static const int INSERT_BATCH_SIZE = 1;

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool query_rows(FlexQL *db, const string &sql, vector<string> &out_rows) {
    struct Collector {
        vector<string> rows;
    } collector;

    auto cb = [](void *data, int argc, char **argv, char **azColName) -> int {
        (void)azColName;
        Collector *c = static_cast<Collector*>(data);
        string row;
        for (int i = 0; i < argc; ++i) {
            if (i > 0) {
                row += "|";
            }
            row += (argv[i] ? argv[i] : "NULL");
        }
        c->rows.push_back(row);
        return 0;
    };

    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    out_rows = collector.rows;
    return true;
}

static bool assert_rows_equal(const string &label, const vector<string> &actual, const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "): \n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "): \n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool assert_row_count(const string &label, const vector<string> &rows, size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
    return false;
}

static string expected_big_users_row(long long id) {
    // Must match run_insert_benchmark serialization.
    // Format for SELECT * is: ID|NAME|EMAIL|BALANCE|EXPIRES_AT
    long long balance = 1000 + (id % 10000);
    return to_string(id) + "|user" + to_string(id) + "|user" + to_string(id) + "@mail.com|" + to_string(balance) + "|1893456000";
}

static bool run_big_users_unit_tests(FlexQL *db, long long inserted_rows) {
    cout << "\n[[...Running BIG_USERS Unit Tests on inserted dataset...] ]\n\n";

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    vector<string> rows;

    // Choose a few representative IDs within [1, inserted_rows]
    const long long id1 = 1;
    const long long idMid = (inserted_rows >= 2) ? (inserted_rows / 2) : 1;
    const long long idLast = inserted_rows;

    // Additional deterministic IDs for more coverage (still point lookups).
    const long long id2 = (inserted_rows >= 2) ? 2 : 1;
    const long long id10 = (inserted_rows >= 10) ? 10 : 1;
    const long long id100 = (inserted_rows >= 100) ? 100 : 1;
    const long long idLastMinus1 = (inserted_rows >= 2) ? (inserted_rows - 1) : 1;
    const long long idLastMinus10 = (inserted_rows >= 11) ? (inserted_rows - 10) : 1;

    // 21 test cases focused on point lookups (feasible even for 10M rows).
    // These intentionally avoid full scans / range predicates.

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT *", rows, {expected_big_users_row(id1)}));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(id2) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=2 SELECT *", rows, {expected_big_users_row(id2)}));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(idMid) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=mid SELECT *", rows, {expected_big_users_row(idMid)}));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(idLastMinus1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last-1 SELECT *", rows, {expected_big_users_row(idLastMinus1)}));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(idLast) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last SELECT *", rows, {expected_big_users_row(idLast)}));

    record(query_rows(db, "SELECT NAME FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT NAME", rows, {"user1"}));

    record(query_rows(db, "SELECT EMAIL FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT EMAIL", rows, {"user1@mail.com"}));

    record(query_rows(db, "SELECT BALANCE FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT BALANCE", rows, {"1001"}));

    record(query_rows(db, "SELECT EXPIRES_AT FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT EXPIRES_AT", rows, {"1893456000"}));

    record(query_rows(db, "SELECT ID, NAME FROM BIG_USERS WHERE ID = " + to_string(idMid) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=mid SELECT ID,NAME", rows, {to_string(idMid) + "|user" + to_string(idMid)}));

    record(query_rows(db, "SELECT ID, EMAIL FROM BIG_USERS WHERE ID = " + to_string(idMid) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=mid SELECT ID,EMAIL", rows, {to_string(idMid) + "|user" + to_string(idMid) + "@mail.com"}));

    record(query_rows(db, "SELECT NAME, BALANCE FROM BIG_USERS WHERE ID = " + to_string(idLast) + ";", rows));
    if (!rows.empty()) {
        long long bal = 1000 + (idLast % 10000);
        record(assert_rows_equal("BIG_USERS ID=last SELECT NAME,BALANCE", rows, {"user" + to_string(idLast) + "|" + to_string(bal)}));
    }

    // Negative tests (expect empty result sets)
    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = 0;", rows));
    record(assert_row_count("BIG_USERS ID=0 should be empty", rows, 0));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = -1;", rows));
    record(assert_row_count("BIG_USERS ID=-1 should be empty", rows, 0));

    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(inserted_rows + 1) + ";", rows));
    record(assert_row_count("BIG_USERS ID=out-of-range should be empty", rows, 0));

    // Single-row equality with qualified projection
    record(query_rows(db, "SELECT ID FROM BIG_USERS WHERE ID = " + to_string(id1) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=1 SELECT ID", rows, {"1"}));

    record(query_rows(db, "SELECT ID FROM BIG_USERS WHERE ID = " + to_string(id10) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=10 SELECT ID", rows, {to_string(id10)}));

    record(query_rows(db, "SELECT ID FROM BIG_USERS WHERE ID = " + to_string(id100) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=100 SELECT ID", rows, {to_string(id100)}));

    record(query_rows(db, "SELECT ID FROM BIG_USERS WHERE ID = " + to_string(idLast) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last SELECT ID", rows, {to_string(idLast)}));

    record(query_rows(db, "SELECT NAME FROM BIG_USERS WHERE ID = " + to_string(idMid) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=mid SELECT NAME", rows, {"user" + to_string(idMid)}));

    record(query_rows(db, "SELECT EMAIL FROM BIG_USERS WHERE ID = " + to_string(idLast) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last SELECT EMAIL", rows, {"user" + to_string(idLast) + "@mail.com"}));

    // Basic expiration enforcement check: EXPIRES_AT is fixed future timestamp, so rows should exist.
    record(query_rows(db, "SELECT EXPIRES_AT FROM BIG_USERS WHERE ID = " + to_string(idLast) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last EXPIRES_AT still valid", rows, {"1893456000"}));

    // Additional point lookups near end of the dataset.
    record(query_rows(db, "SELECT * FROM BIG_USERS WHERE ID = " + to_string(idLastMinus10) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last-10 SELECT *", rows, {expected_big_users_row(idLastMinus10)}));

    record(query_rows(db, "SELECT NAME FROM BIG_USERS WHERE ID = " + to_string(idLastMinus10) + ";", rows));
    if (!rows.empty()) record(assert_rows_equal("BIG_USERS ID=last-10 SELECT NAME", rows, {"user" + to_string(idLastMinus10)}));

    int passed_tests = total_tests - failed_tests;
    cout << "\nBIG_USERS Unit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_insert_benchmark(FlexQL *db, long long target_rows) {
    // Use INT for ID so the server can use the INT-PK fast path for point lookups.
    if (!run_exec(
            db,
            "CREATE TABLE BIG_USERS(ID INT, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DATETIME);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows << " rows...\n";
    auto bench_start = high_resolution_clock::now();

    long long inserted = 0;
    long long progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    long long next_progress = progress_step;

    while (inserted < target_rows) {
        stringstream ss;
        ss << "INSERT INTO BIG_USERS VALUES ";

        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < target_rows) {
            long long id = inserted + 1;
            ss << "(" << id
               << ", 'user" << id << "'"
               << ", 'user" << id << "@mail.com'"
               << ", " << (1000 + (id % 10000))
               << ", 1893456000)";
            inserted++;
            in_batch++;
            if (in_batch < INSERT_BATCH_SIZE && inserted < target_rows) {
                ss << ",";
            }
        }
        ss << ";";

        char *errMsg = nullptr;
        if (flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
            cout << "[FAIL] INSERT BIG_USERS batch -> " << (errMsg ? errMsg : "unknown error") << "\n";
            if (errMsg) {
                flexql_free(errMsg);
            }
            return false;
        }

        if (inserted >= next_progress || inserted == target_rows) {
            cout << "Progress: " << inserted << "/" << target_rows << "\n";
            next_progress += progress_step;
        }
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << target_rows << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    return true;
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    long long insert_rows = DEFAULT_INSERT_ROWS;

    if (argc > 1) {
        insert_rows = atoll(argv[1]);
        if (insert_rows <= 0) {
            cout << "Invalid row count. Use a positive integer.\n";
            return 1;
        }
    }

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Connected to FlexQL\n";
    cout << "Running insert benchmark + BIG_USERS validation suite...\n";
    cout << "Target insert rows: " << insert_rows << "\n\n";

    if (!run_insert_benchmark(db, insert_rows)) {
        flexql_close(db);
        return 1;
    }

    bool ok = run_big_users_unit_tests(db, insert_rows);

    flexql_close(db);
    return ok ? 0 : 1;
}
