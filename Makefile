CXX := g++
CXXFLAGS := -O3 -std=c++17 -Wall -Wextra -pedantic
LDFLAGS :=

PTHREAD_LIBS := -lpthread

all: server benchmark client

server: server_full.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(PTHREAD_LIBS)

benchmark: benchmark_flexql.cpp flexql_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(PTHREAD_LIBS)

benchmark_big_users: benchmark_big_users.cpp flexql_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(PTHREAD_LIBS)

client: client_repl.cpp flexql_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^ $(PTHREAD_LIBS)

clean:
	rm -f server benchmark benchmark_big_users client flexql.db

.PHONY: all clean
