// HashTable.h

#ifndef HASHTABLE_H   // include gaurd to prevents errors tht may occur if we include file more than once
#define HASHTABLE_H

#include <string>
#include <vector>
#include <mutex>
#include <fstream>
// Represents one key-value pair in a linked list.
enum class EntryState { EMPTY, OCCUPIED, DELETED };

struct Entry {
    std::string key;
    std::string value;
    EntryState state = EntryState::EMPTY;
};

class CustomHashTable {
private:
    std::vector<Entry> buckets; // Our array of pointers to Nodes (the "shelves")
    size_t capacity;  
    size_t current_size;          // The total number of buckets
    std::ofstream log_file;

    mutable std::mutex table_mutex; 
    
    void rehash();
    // The function to convert a key into a bucket index.
    size_t hash1(const std::string& key) const;
    size_t hash2(const std::string& key) const;

public:
    explicit CustomHashTable(const std::string& log_filename, size_t initial_capacity = 2);
    ~CustomHashTable();
    void set(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    bool del(const std::string& key);
};

#endif // HASHTABLE_H