#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <vector>
#include <string>

class BloomFilter {
private:
    std::vector<bool> bit_array;
    int num_hashes;

public:
    BloomFilter(int size, int num_hashes);
    void add(const std::string& key);
    bool contains(const std::string& key) const;
};

#endif // BLOOMFILTER_H