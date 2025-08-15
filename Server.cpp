// Server.cpp

#include "Server.h"
#include <iostream>
#include <vector>
#include <sstream>
#include <atomic>

// A global, atomic counter for the number of commands processed
std::atomic<int> command_counter(0);
// Create a snapshot every 10 write commands (for easier testing)
const int SNAPSHOT_INTERVAL = 10; 


// Constructor for Winsock initialization
Server::Server(CustomHashTable& store) : data_store(store), is_running(false), server_socket(INVALID_SOCKET) {
#ifdef _WIN32
    WSADATA wsaData;
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result != 0) {
        std::cerr << "WSAStartup failed: " << result << std::endl;
        exit(1);
    }
#endif
}

// Destructor for Winsock cleanup
Server::~Server() {
    is_running = false;
    if (server_socket != INVALID_SOCKET) {
        closesocket(server_socket);
    }

    for (auto& t : client_threads) {
        if (t.joinable()) {
            t.detach();
        }
    }
#ifdef _WIN32
    WSACleanup();
#endif
}

// Static method to handle a single client
void Server::handle_client(SOCKET client_socket, CustomHashTable& store) { // Changed int to SOCKET
    std::cout << "[Server] New client connected. Socket: " << client_socket << std::endl;
    char buffer[1024] = {0};

    while (true) {
        memset(buffer, 0, 1024);
        // Changed read() to recv()
        int bytes_read = recv(client_socket, buffer, 1024, 0);

        if (bytes_read <= 0) {
            std::cout << "[Server] Client disconnected. Socket: " << client_socket << std::endl;
            closesocket(client_socket);
            return;
        }

        std::string input_line(buffer, bytes_read);
        input_line.erase(input_line.find_last_not_of("\r\n") + 1);

        if (input_line == "exit") {
            std::string goodbye = "Goodbye!\r\n";
            send(client_socket, goodbye.c_str(), goodbye.length(), 0);
            std::cout << "[Server] Client disconnected. Socket: " << client_socket << std::endl;
            closesocket(client_socket);
            return;
        }

        // --- Command processing logic ---
        std::stringstream ss(input_line);
        std::string token;
        std::vector<std::string> tokens;
        while (ss >> token) {
            tokens.push_back(token);
        }

        std::string response;
        // FIX: The main logic block should check if tokens is NOT empty.
        if (!tokens.empty()) {
            const std::string& command = tokens[0];

            // Trigger snapshot logic for write commands
            if (command == "set" || command == "del") {
                // Increment the counter for every write operation
                int current_count = command_counter.fetch_add(1);
                
                // Check if it's time to create a snapshot
                if (current_count > 0 && current_count % SNAPSHOT_INTERVAL == 0) {
                    store.create_snapshot();
                }
            }

            // Process the actual command
            if (command == "set") {
                if (tokens.size() != 3) response = "ERROR: Usage: set <key> <value>\r\n";
                else { store.set(tokens[1], tokens[2]); response = "OK\r\n"; }
            } else if (command == "get") {
                if (tokens.size() != 2) response = "ERROR: Usage: get <key>\r\n";
                else response = store.get(tokens[1]) + "\r\n";
            } else if (command == "del") {
                if (tokens.size() != 2) response = "ERROR: Usage: del <key>\r\n";
                else response = store.del(tokens[1]) ? "(integer) 1\r\n" : "(integer) 0\r\n";
            } else {
                response = "ERROR: Unknown command '" + command + "'\r\n";
            }
        } else {
            // Handle empty input line
            response = "\r\n";
        }
        
        send(client_socket, response.c_str(), response.length(), 0);
    }
}

// Main server loop
void Server::run() {
    sockaddr_in server_address;

    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET) {
        std::cerr << "Socket creation failed with error: " << WSAGetLastError() << std::endl;
        exit(EXIT_FAILURE);
    }

    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(PORT);

    if (bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) == SOCKET_ERROR) {
        std::cerr << "Bind failed with error: " << WSAGetLastError() << std::endl;
        closesocket(server_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Listen failed with error: " << WSAGetLastError() << std::endl;
        closesocket(server_socket);
        exit(EXIT_FAILURE);
    }

    std::cout << "[Server] Server is listening on port " << PORT << "..." << std::endl;
    is_running = true;

    while (is_running) {
        SOCKET client_socket;
        sockaddr_in client_address;
        int addrlen = sizeof(client_address);

        if ((client_socket = accept(server_socket, (struct sockaddr *)&client_address, (socklen_t*)&addrlen)) == INVALID_SOCKET) {
            std::cerr << "Accept failed with error: " << WSAGetLastError() << std::endl;
            continue;
        }
        
        client_threads.emplace_back(handle_client, client_socket, std::ref(data_store));
    }
}
