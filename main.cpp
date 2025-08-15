// main.cpp

#include "HashTable.h"
#include "Server.h"
#include <iostream>

int main() {
    try {
        // 1. Create the shared data store instance
        CustomHashTable data_store("kv_store.log");

        // 2. Create and run the server, passing the data store to it
        Server kv_server(data_store);
        kv_server.run();

    } catch (const std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    return 0;
}