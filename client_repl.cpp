#include <iostream>
#include <string>
#include <sstream>

#include "flexql.h"

static int print_callback(void *data, int argc, char **argv, char **azColName) {
    (void)data;
    for (int i = 0; i < argc; i++) {
        const char *name = azColName && azColName[i] ? azColName[i] : "";
        const char *val = argv && argv[i] ? argv[i] : "NULL";
        std::cout << name << " = " << val << "\n";
    }
    std::cout << "\n";
    return 0;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 9000;

    if (argc >= 2) {
        host = argv[1];
    }
    if (argc >= 3) {
        port = std::atoi(argv[2]);
    }

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot open FlexQL\n";
        return 1;
    }

    std::cout << "Connected to FlexQL server\n";

    std::string line;
    std::string sql;
    while (true) {
        std::cout << "flexql> ";
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line == ".exit" || line == ".quit") {
            break;
        }

        sql += line;
        sql += "\n";

        // execute when we see a semicolon
        if (sql.find(';') == std::string::npos) {
            continue;
        }

        char *errMsg = nullptr;
        int rc = flexql_exec(db, sql.c_str(), print_callback, nullptr, &errMsg);
        if (rc != FLEXQL_OK) {
            std::cerr << (errMsg ? errMsg : "unknown error") << "\n";
            if (errMsg) {
                flexql_free(errMsg);
            }
        }
        sql.clear();
    }

    flexql_close(db);
    std::cout << "Connection closed\n";
    return 0;
}
