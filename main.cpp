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
        tokens.push_back(token); // Add the extracted word to our vector.
    }
    return tokens;
}

int main() {
    CustomHashTable data_store("kv_store.log");
    cout << "Welcome to KV-Store! Type 'exit' to quit." << endl;

    // This is our main program loop. It will run forever until we 'break' out of it.
    while (true) {
        cout << "> "; 

        string input_line;
        getline(cin, input_line);  //#

        // Check if the user wants to quit.
        if (input_line == "exit") {
            cout << "Goodbye!" << std::endl;
            break; // Exit the while loop.
        }
         vector<string> tokens = parse_input(input_line);

        // If the user just pressed Enter, tokens will be empty.
        // 'continue' skips the rest of the loop and starts from the top.
        if (tokens.empty()) {
            continue;
        }

            const string& command = tokens[0];

        // *** NEW: Command processing logic ***
        if (command == "set") {
            if (tokens.size() != 3) {
                cout << "ERROR: Usage: set <key> <value>" << endl;
            } else {
                // This now works!
                data_store.set(tokens[1], tokens[2]);
                cout << "OK" << endl;
            }
        } else if (command == "get") {
            if (tokens.size() != 2) {
                cout << "ERROR: Usage: get <key>" <<endl;
            } else {
                // This now works!
                cout << data_store.get(tokens[1]) << endl;
            }
        } else if (command == "del") {
            if (tokens.size() != 2) {
                cout << "ERROR: Usage: del <key>" << endl;
            } else {
                // This now works!
                if (data_store.del(tokens[1])) {
                    cout << "(integer) 1" << endl;
                } else {
                    cout << "(integer) 0" << endl;
                }
            }
        }  else {
            cout << "ERROR: Unknown command '" << command << "'" << endl;
        }
    }

    // The program will end when it reaches the end of the main function.
    return 0;
}