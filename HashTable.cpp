#include "HashTable.h"
#include <iostream>
#include <stdexcept>
#include <fstream>
#include <sstream> // Required for std::stringstream

// Use a lower max load factor for open addressing to ensure empty slots for probing.
const double MAX_LOAD_FACTOR = 0.7;
const std::string SNAPSHOT_FILENAME = "kv_store.snapshot";

// The constructor initializes the table with empty entries.
// FIX: Renamed the parameter to 'log_file_param' to avoid shadowing the member variable.
CustomHashTable::CustomHashTable(const std::string& log_file_param) : log_filename(log_file_param) {
    this->capacity = 2; // Initial capacity
    this->buckets.resize(this->capacity);
    this->current_size = 0;

    // Try to load from a snapshot first.
    load_from_snapshot();

    // Open the log file in append mode. This must be done AFTER loading.
    // This now correctly refers to the std::ofstream member variable.
    this->log_file.open(log_filename, std::ios::app);
    if (!this->log_file.is_open()) {
        throw std::runtime_error("FATAL: Could not open log file: " + log_filename);
    }
}

void CustomHashTable::load_from_snapshot() {
    std::ifstream snapshot_file(SNAPSHOT_FILENAME, std::ios::binary);
    if (!snapshot_file.is_open()) {
        std::cout << "[SYSTEM] Snapshot not found. Will replay log if it exists." << std::endl;
        // If no snapshot, we try to replay the log instead.
        replay_log();
        return;
    }

    // Read snapshot data
    snapshot_file.read(reinterpret_cast<char*>(&capacity), sizeof(capacity));
    snapshot_file.read(reinterpret_cast<char*>(&current_size), sizeof(current_size));

    buckets.resize(capacity);
    for (size_t i = 0; i < capacity; ++i) {
        snapshot_file.read(reinterpret_cast<char*>(&buckets[i].state), sizeof(EntryState));
        if (buckets[i].state == EntryState::OCCUPIED) {
            size_t key_len, val_len;
            snapshot_file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
            buckets[i].key.resize(key_len);
            snapshot_file.read(&buckets[i].key[0], key_len);

            snapshot_file.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
            buckets[i].value.resize(val_len);
            snapshot_file.read(&buckets[i].value[0], val_len);
        }
    }
    snapshot_file.close();
    std::cout << "[SYSTEM] Successfully loaded state from snapshot." << std::endl;
}

void CustomHashTable::replay_log() {
    std::ifstream existing_log(log_filename);
    if (!existing_log.is_open()) {
        std::cout << "[SYSTEM] No existing log file found. Starting fresh." << std::endl;
        return;
    }
    
    std::cout << "[SYSTEM] Replaying commands from log file..." << std::endl;
    std::string line;
    while (std::getline(existing_log, line)) {
        std::stringstream ss(line);
        std::string command, key, value;
        ss >> command >> key;
        if (command == "set") {
            ss >> value;
            
            // Check if a rehash is needed during log replay.
            if ((double)(current_size + 1) / capacity > MAX_LOAD_FACTOR) {
                rehash();
            }

            // Use the internal set logic directly to avoid re-logging
            size_t index = hash1(key);
            size_t step = hash2(key);
            for(size_t i = 0; i < capacity; ++i) {
                // Find the first available spot (empty or deleted)
                if(buckets[index].state != EntryState::OCCUPIED){
                    buckets[index] = {key, value, EntryState::OCCUPIED};
                    current_size++;
                    break;
                }
                index = (index + step) % capacity;
            }
        } else if (command == "del") {
            // Internal delete logic
            size_t index = hash1(key);
            size_t step = hash2(key);
            for(size_t i = 0; i < capacity; ++i) {
                if(buckets[index].state == EntryState::OCCUPIED && buckets[index].key == key){
                    buckets[index].state = EntryState::DELETED;
                    current_size--;
                    break;
                }
                index = (index + step) % capacity;
            }
        }
    }
    existing_log.close();
}

void CustomHashTable::create_snapshot() {
    std::lock_guard<std::mutex> lock(table_mutex);
    std::cout << "[SYSTEM] Creating snapshot..." << std::endl;

    // 1. Write the current state to a temporary snapshot file
    std::ofstream snapshot_file(SNAPSHOT_FILENAME + ".tmp", std::ios::binary);
    if (!snapshot_file.is_open()) {
        std::cerr << "ERROR: Could not create temporary snapshot file." << std::endl;
        return;
    }

    snapshot_file.write(reinterpret_cast<const char*>(&capacity), sizeof(capacity));
    snapshot_file.write(reinterpret_cast<const char*>(&current_size), sizeof(current_size));

    for (const auto& entry : buckets) {
        snapshot_file.write(reinterpret_cast<const char*>(&entry.state), sizeof(EntryState));
        if (entry.state == EntryState::OCCUPIED) {
            size_t key_len = entry.key.length();
            snapshot_file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            snapshot_file.write(entry.key.c_str(), key_len);

            size_t val_len = entry.value.length();
            snapshot_file.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
            snapshot_file.write(entry.value.c_str(), val_len);
        }
    }
    snapshot_file.close();

    // 2. Atomically replace the old snapshot with the new one
    remove(SNAPSHOT_FILENAME.c_str());
    rename((SNAPSHOT_FILENAME + ".tmp").c_str(), SNAPSHOT_FILENAME.c_str());
    
    // 3. Log Compaction: Close, truncate, and reopen the log file
    if (this->log_file.is_open()) {
        this->log_file.close();
    }
    // Re-open in truncate mode to clear it, then close immediately.
    this->log_file.open(log_filename, std::ios::trunc); 
    this->log_file.close();
    // Re-open in append mode for future commands.
    this->log_file.open(log_filename, std::ios::app);

    std::cout << "[SYSTEM] Snapshot created and log compacted successfully." << std::endl;
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
    if (capacity > 1) {
        return (hash % (capacity - 1)) + 1;
    }
    return 1;
}

// The 'set' method was mostly correct and is kept here.
void CustomHashTable::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(table_mutex);
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
    std::lock_guard<std::mutex> lock(table_mutex);

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
    std::lock_guard<std::mutex> lock(table_mutex);

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
        if (old_buckets[i].state == EntryState::OCCUPIED) {
            const std::string& key = old_buckets[i].key;
            const std::string& value = old_buckets[i].value;

             // The public `set` method can't be used here as it would try to lock the mutex again,
    // causing a deadlock. So we have to re-implement the set logic without the lock.
            size_t index = hash1(key);
            size_t step = hash2(key);
            for (size_t j = 0; j < capacity; ++j) {
                if (buckets[index].state != EntryState::OCCUPIED) {
                    buckets[index] = {key, value, EntryState::OCCUPIED};
                    current_size++;
                    break;
                }
                index = (index + step) % capacity;
            }
        }
    }
}
