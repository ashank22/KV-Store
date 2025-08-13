#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <any>
#include "BloomFilter.h"

// Represents one key-value pair in a linked list.
enum class EntryState { EMPTY, OCCUPIED, DELETED };

template <typename K, typename V>
struct Entry {
    K key;
    V value;
    EntryState state = EntryState::EMPTY;
};

template <typename K, typename V>
class CustomHashTable {
private:
    std::vector<Entry<K, V>> buckets;
    size_t capacity;
    size_t current_size;
    std::ofstream log_file;
    BloomFilter bloom_filter;

    void rehash();
    size_t hash1(const K& key) const;
    size_t hash2(const K& key) const;

public:
    explicit CustomHashTable(const std::string& log_filename, size_t initial_capacity = 2);
    ~CustomHashTable();
    void set(const K& key, const V& value);
    V get(const K& key);
    bool del(const K& key);
    void load_from_log();
};

#endif // HASHTABLE_H