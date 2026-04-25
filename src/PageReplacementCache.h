/*
 * ================================================================
 * PageReplacementCache.h
 * Week 6: Memory Management (Page Replacement Algorithms)
 * ================================================================
 */

#ifndef PAGE_REPLACEMENT_CACHE_H
#define PAGE_REPLACEMENT_CACHE_H

#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <algorithm>
#include <iomanip>

class FIFOCache {
private:
    int capacity;
    int page_faults;
    int hits;
    std::list<std::string> frames;

public:
    FIFOCache(int cap) : capacity(cap), page_faults(0), hits(0) {}

    void access(const std::string& account) {
        if (std::find(frames.begin(), frames.end(), account) != frames.end()) {
            hits++;
        } else {
            page_faults++;
            if (frames.size() == capacity) {
                frames.pop_front();
            }
            frames.push_back(account);
        }
    }

    void printStats() const {
        int total = hits + page_faults;
        double hit_rate = total == 0 ? 0.0 : (double)hits / total * 100.0;
        std::cout << "  FIFO Cache    | Faults: " << std::setw(2) << page_faults 
                  << " | Hits: " << std::setw(2) << hits 
                  << " | Hit Rate: " << std::fixed << std::setprecision(2) << hit_rate << "%\n";
    }
};

class LRUCache {
private:
    int capacity;
    int page_faults;
    int hits;
    std::list<std::string> frames;

public:
    LRUCache(int cap) : capacity(cap), page_faults(0), hits(0) {}

    void access(const std::string& account) {
        auto it = std::find(frames.begin(), frames.end(), account);
        if (it != frames.end()) {
            hits++;
            frames.erase(it);
            frames.push_front(account); // Move to most recently used
        } else {
            page_faults++;
            if (frames.size() == capacity) {
                frames.pop_back(); // Remove least recently used
            }
            frames.push_front(account);
        }
    }

    void printStats() const {
        int total = hits + page_faults;
        double hit_rate = total == 0 ? 0.0 : (double)hits / total * 100.0;
        std::cout << "  LRU Cache     | Faults: " << std::setw(2) << page_faults 
                  << " | Hits: " << std::setw(2) << hits 
                  << " | Hit Rate: " << std::fixed << std::setprecision(2) << hit_rate << "%\n";
    }
};

class OptimalCache {
private:
    int capacity;
    int page_faults;
    int hits;
    std::vector<std::string> frames;

public:
    OptimalCache(int cap) : capacity(cap), page_faults(0), hits(0) {}

    void simulate(const std::vector<std::string>& ref_string) {
        for (size_t i = 0; i < ref_string.size(); i++) {
            std::string account = ref_string[i];
            auto it = std::find(frames.begin(), frames.end(), account);
            
            if (it != frames.end()) {
                hits++;
            } else {
                page_faults++;
                if (frames.size() < capacity) {
                    frames.push_back(account);
                } else {
                    // Find the page to replace (the one that won't be used for the longest time)
                    int farthest = i;
                    int replace_idx = -1;
                    
                    for (size_t j = 0; j < frames.size(); j++) {
                        int next_use = -1;
                        for (size_t k = i + 1; k < ref_string.size(); k++) {
                            if (frames[j] == ref_string[k]) {
                                next_use = k;
                                break;
                            }
                        }
                        
                        if (next_use == -1) {
                            replace_idx = j;
                            break; // This page is never used again
                        }
                        
                        if (next_use > farthest) {
                            farthest = next_use;
                            replace_idx = j;
                        }
                    }
                    frames[replace_idx] = account;
                }
            }
        }
    }

    void printStats() const {
        int total = hits + page_faults;
        double hit_rate = total == 0 ? 0.0 : (double)hits / total * 100.0;
        std::cout << "  Optimal Cache | Faults: " << std::setw(2) << page_faults 
                  << " | Hits: " << std::setw(2) << hits 
                  << " | Hit Rate: " << std::fixed << std::setprecision(2) << hit_rate << "%\n";
    }
};

#endif // PAGE_REPLACEMENT_CACHE_H