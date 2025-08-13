#include "HashTable.h"
#include <iostream>
#include <stdexcept>
#include <sstream>

const double MAX_LOAD_FACTOR = 0.7;

template <typename K, typename V>
CustomHashTable<K, V>::CustomHashTable(const std::string& log_filename, size_t initial_capacity)
    : bloom_filter(1000, 5) {
    log_file.open(log_filename, std::ios::app);
    if (!log_file.is_open()) {
        throw std::runtime_error("FATAL: Could not open log file: " + log_filename);
    }

    this->capacity = initial_capacity;
    this->buckets.resize(this->capacity);
    this->current_size = 0;
    load_from_log();
}

template <typename K, typename V>
CustomHashTable<K, V>::~CustomHashTable() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

template <typename K, typename V>
void CustomHashTable<K, V>::load_from_log() {
    std::ifstream log_stream("kv_store.log");
    if (!log_stream.is_open()) {
        return; // No log file to load
    }

    std::string line;
    while (std::getline(log_stream, line)) {
        std::stringstream ss(line);
        std::string command, key, value;
        ss >> command >> key;
        if (command == "set") {
            ss >> value;
            set(key, value);
        } else if (command == "del") {
            del(key);
        }
    }
    log_stream.close();
}

template <typename K, typename V>
size_t CustomHashTable<K, V>::hash1(const K& key) const {
    return std::hash<K>{}(key) % capacity;
}

template <typename K, typename V>
size_t CustomHashTable<K, V>::hash2(const K& key) const {
    return (std::hash<K>{}(key) % (capacity - 1)) + 1;
}

template <typename K, typename V>
void CustomHashTable<K, V>::set(const K& key, const V& value) {
    log_file << "set " << key << " " << value << std::endl;
    bloom_filter.add(key);

    if ((double)(current_size + 1) / capacity > MAX_LOAD_FACTOR) {
        rehash();
    }

    size_t index = hash1(key);
    size_t step = hash2(key);
    size_t first_deleted = -1;

    for (size_t i = 0; i < capacity; ++i) {
        Entry<K, V>& entry = buckets[index];

        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            entry.value = value;
            return;
        }
        if (entry.state == EntryState::DELETED) {
            if (first_deleted == (size_t)-1) {
                first_deleted = index;
            }
        }
        if (entry.state == EntryState::EMPTY) {
            size_t insert_pos = (first_deleted != (size_t)-1) ? first_deleted : index;
            buckets[insert_pos] = {key, value, EntryState::OCCUPIED};
            current_size++;
            return;
        }
        index = (index + step) % capacity;
    }
}

template <typename K, typename V>
V CustomHashTable<K, V>::get(const K& key) {
    if (!bloom_filter.contains(key)) {
        return V{}; // Return default-constructed value
    }

    size_t index = hash1(key);
    size_t step = hash2(key);

    for (size_t i = 0; i < capacity; ++i) {
        const Entry<K, V>& entry = buckets[index];

        if (entry.state == EntryState::EMPTY) {
            return V{};
        }
        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            return entry.value;
        }
        index = (index + step) % capacity;
    }

    return V{};
}

template <typename K, typename V>
bool CustomHashTable<K, V>::del(const K& key) {
    if (!bloom_filter.contains(key)) {
        return false;
    }

    log_file << "del " << key << std::endl;
    size_t index = hash1(key);
    size_t step = hash2(key);

    for (size_t i = 0; i < capacity; ++i) {
        Entry<K, V>& entry = buckets[index];

        if (entry.state == EntryState::EMPTY) {
            return false;
        }
        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            entry.state = EntryState::DELETED;
            entry.key = K{};
            entry.value = V{};
            current_size--;
            return true;
        }
        index = (index + step) % capacity;
    }

    return false;
}

template <typename K, typename V>
void CustomHashTable<K, V>::rehash() {
    std::cout << "[SYSTEM] Load factor exceeded. Rehashing from " << capacity << " to " << capacity * 2 << " buckets." << std::endl;

    size_t old_capacity = capacity;
    std::vector<Entry<K, V>> old_buckets = std::move(buckets);

    capacity *= 2;
    buckets.assign(capacity, Entry<K, V>());
    current_size = 0;

    for (size_t i = 0; i < old_capacity; ++i) {
        if (old_buckets[i].state == EntryState::OCCUPIED) {
            set(old_buckets[i].key, old_buckets[i].value);
        }
    }
}

// Explicit template instantiation
template class CustomHashTable<std::string, std::string>;