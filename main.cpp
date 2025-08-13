#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "HashTable.h"

using namespace std;

vector<string> parse_input(const string& input_line) {
    stringstream ss(input_line);
    string token;
    vector<string> tokens;
    while (ss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

int main() {
    CustomHashTable<string, string> data_store("kv_store.log");
    cout << "Welcome to KV-Store! Type 'exit' to quit." << endl;

    while (true) {
        cout << "> ";

        string input_line;
        getline(cin, input_line);

        if (input_line == "exit") {
            cout << "Goodbye!" << std::endl;
            break;
        }
        vector<string> tokens = parse_input(input_line);

        if (tokens.empty()) {
            continue;
        }

        const string& command = tokens[0];

        if (command == "set") {
            if (tokens.size() != 3) {
                cout << "ERROR: Usage: set <key> <value>" << endl;
            } else {
                data_store.set(tokens[1], tokens[2]);
                cout << "OK" << endl;
            }
        } else if (command == "get") {
            if (tokens.size() != 2) {
                cout << "ERROR: Usage: get <key>" << endl;
            } else {
                string value = data_store.get(tokens[1]);
                if (value.empty()) {
                    cout << "(nil)" << endl;
                } else {
                    cout << value << endl;
                }
            }
        } else if (command == "del") {
            if (tokens.size() != 2) {
                cout << "ERROR: Usage: del <key>" << endl;
            } else {
                if (data_store.del(tokens[1])) {
                    cout << "(integer) 1" << endl;
                } else {
                    cout << "(integer) 0" << endl;
                }
            }
        } else {
            cout << "ERROR: Unknown command '" << command << "'" << endl;
        }
    }

    return 0;
}