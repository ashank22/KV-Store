// Server.h

#ifndef SERVER_H
#define SERVER_H

#include "HashTable.h"
#include <thread>
#include <vector>

// Add platform-specific headers for networking
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    // Link with Ws2_32.lib
    #pragma comment (lib, "ws2_32.lib")
#else
    #include <unistd.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
#endif


// Define a port for our server to listen on
#define PORT 8080

class Server {
private:
    CustomHashTable& data_store;
    SOCKET server_socket; // Changed from int to SOCKET
    bool is_running;
    std::vector<std::thread> client_threads;

    static void handle_client(SOCKET client_socket, CustomHashTable& store); // Changed from int to SOCKET

public:
    Server(CustomHashTable& store);
    ~Server();
    void run();
};

#endif // SERVER_H