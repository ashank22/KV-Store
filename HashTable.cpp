#include "HashTable.h"
#include <iostream>
#include <stdexcept>
// Use a lower max load factor for open addressing to ensure empty slots for probing.
const double MAX_LOAD_FACTOR = 0.7;

// The constructor initializes the table with empty entries.
CustomHashTable::CustomHashTable(const std::string& log_filename, size_t initial_capacity) {
    // Open the log file in append mode.
    log_file.open(log_filename, std::ios::app);
    if (!log_file.is_open()) {
        // If the file can't be opened, we can't guarantee durability, so we should stop.
        throw std::runtime_error("FATAL: Could not open log file: " + log_filename);
    }

    this->capacity = initial_capacity;
    this->buckets.resize(this->capacity);
    this->current_size = 0;
}

// The destructor is empty because std::vector handles all memory management.
CustomHashTable::~CustomHashTable() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

// First hash function to determine the initial bucket.
size_t CustomHashTable::hash1(const std::string& key) const {
    size_t hash = 5381;
    for (char c : key) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % capacity;
}

// Second hash function to determine the probe step size.
// Must be a different algorithm and must not return 0.
size_t CustomHashTable::hash2(const std::string& key) const {
    size_t hash = 0;
    for (char c : key) {
        hash = c + (hash << 6) + (hash << 16) - hash;
    }
    // Ensure the step is non-zero. R - (key % R) is a common technique.
    return (hash % (capacity - 1)) + 1;
}

// The 'set' method was mostly correct and is kept here.
void CustomHashTable::set(const std::string& key, const std::string& value) {
    log_file << "set " << key << " " << value << std::endl;
    // Check if a rehash is needed before inserting a new element.
    if ((double)(current_size + 1) / capacity > MAX_LOAD_FACTOR) {
        rehash();
    }

    size_t index = hash1(key);
    size_t step = hash2(key);
    size_t first_deleted = -1; // Sentinel value to track the first tombstone

    for (size_t i = 0; i < capacity; ++i) {
        Entry& entry = buckets[index]; // Use a reference to modify the actual entry

        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            entry.value = value; // Update existing key
            return;
        }
        if (entry.state == EntryState::DELETED) {
            if (first_deleted == (size_t)-1) {
                first_deleted = index; // Found the first tombstone, save its position
            }
        }
        if (entry.state == EntryState::EMPTY) {
            // Found a truly empty spot. Use it, or the first tombstone if we found one.
            size_t insert_pos = (first_deleted != (size_t)-1) ? first_deleted : index;
            buckets[insert_pos] = {key, value, EntryState::OCCUPIED};
            current_size++;
            return;
        }
        index = (index + step) % capacity; // Probe the next spot
    }
}

// *** CORRECTED 'get' method ***
std::string CustomHashTable::get(const std::string& key) {
    size_t index = hash1(key);
    size_t step = hash2(key);

    for (size_t i = 0; i < capacity; ++i) {
        const Entry& entry = buckets[index]; // Use a const reference to read

        // If we hit an empty slot, the key cannot be in the table.
        if (entry.state == EntryState::EMPTY) {
            return "(nil)";
        }
        // If we find an occupied slot with the correct key, we've found our value.
        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            return entry.value;
        }
        // If the slot is deleted or occupied by another key, continue probing.
        index = (index + step) % capacity;
    }

    // If we search the whole table and don't find it, it's not there.
    return "(nil)";
}

// *** CORRECTED 'del' method ***
bool CustomHashTable::del(const std::string& key) {
      log_file << "del " << key << std::endl;
    size_t index = hash1(key);
    size_t step = hash2(key);

    for (size_t i = 0; i < capacity; ++i) {
        Entry& entry = buckets[index]; // Use a reference to modify the entry

        if (entry.state == EntryState::EMPTY) {
            return false; // Key not found
        }
        if (entry.state == EntryState::OCCUPIED && entry.key == key) {
            entry.state = EntryState::DELETED; // Mark with a tombstone
            entry.key = "";   // Clear data to save space
            entry.value = "";
            current_size--;
            return true;
        }
        index = (index + step) % capacity;
    }

    return false; // Key not found after searching the whole table
}

// *** CORRECTED 'rehash' method ***
void CustomHashTable::rehash() {
    std::cout << "[SYSTEM] Load factor exceeded. Rehashing from " << capacity << " to " << capacity * 2 << " buckets." << std::endl;

    size_t old_capacity = capacity;
    std::vector<Entry> old_buckets = std::move(buckets); // Efficiently move the old data

    // Double the capacity, create a new empty table
    capacity *= 2;
    buckets.assign(capacity, Entry()); // .assign is a clean way to resize and fill
    current_size = 0; // The 'set' method will recount the size

    // Iterate through all buckets of the OLD table
    for (size_t i = 0; i < old_capacity; ++i) {
        // Only re-insert entries that were actually occupied
        if (old_buckets[i].state == EntryState::OCCUPIED) {
            // Use the public 'set' method to re-insert the entry into the new, larger table.
            // 'set' will correctly handle hashing with the new capacity.
            set(old_buckets[i].key, old_buckets[i].value);
        }
    }
}