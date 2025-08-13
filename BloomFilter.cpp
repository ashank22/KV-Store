#include "BloomFilter.h"
#include <functional>

BloomFilter::BloomFilter(int size, int num_hashes) {
    bit_array.resize(size, false);
    this->num_hashes = num_hashes;
}

void BloomFilter::add(const std::string& key) {
    for (int i = 0; i < num_hashes; ++i) {
        size_t hash_val = std::hash<std::string>{}(key + std::to_string(i));
        bit_array[hash_val % bit_array.size()] = true;
    }
}

bool BloomFilter::contains(const std::string& key) const {
    for (int i = 0; i < num_hashes; ++i) {
        size_t hash_val = std::hash<std::string>{}(key + std::to_string(i));
        if (!bit_array[hash_val % bit_array.size()]) {
            return false;
        }
    }
    return true;
}