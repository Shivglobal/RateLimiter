#include <iostream>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>
#include <vector>

class RateLimiter {
public:
    /**
     * @param max_requests Maximum number of requests allowed in the window.
     * @param window_seconds Time window in seconds.
     * @param num_shards Number of lock shards to reduce mutex contention (must be > 0).
     */
    RateLimiter(int max_requests, double window_seconds, size_t num_shards = 16)
        : capacity_(static_cast<double>(max_requests)),
          refill_rate_(static_cast<double>(max_requests) / window_seconds),
          num_shards_(num_shards),
          shards_(num_shards) {}

    // Check if a client is allowed to make a request
    bool allowRequest(uint32_t client_id) {
        auto& shard = getShard(client_id);
        std::lock_guard<std::mutex> lock(shard.mutex);

        auto now = std::chrono::steady_clock::now();
        auto& bucket = shard.clients[client_id];

        // If this is a new client, initialize their bucket and consume 1 token immediately
        if (bucket.last_refill == std::chrono::steady_clock::time_point{}) {
            bucket.tokens = capacity_ - 1.0;
            bucket.last_refill = now;
            return true;
        }

        // 1. Calculate time elapsed since the last refill
        std::chrono::duration<double> elapsed = now - bucket.last_refill;
        bucket.last_refill = now;

        // 2. Refill tokens based on elapsed time and refill rate (capped at capacity)
        bucket.tokens = std::min(capacity_, bucket.tokens + (elapsed.count() * refill_rate_));

        // 3. Consume a token if available
        if (bucket.tokens >= 1.0) {
            bucket.tokens -= 1.0;
            return true; // Request allowed
        }

        return false; // Rate limited
    }

    // Prevents memory leaks by clearing out inactive clients across all shards
    void cleanupInactiveClients(std::chrono::seconds max_inactive_time) {
        auto now = std::chrono::steady_clock::now();

        for (auto& shard : shards_) {
            std::lock_guard<std::mutex> lock(shard.mutex);
            for (auto it = shard.clients.begin(); it != shard.clients.end(); ) {
                std::chrono::duration<double> inactive_duration = now - it->second.last_refill;
                if (inactive_duration > max_inactive_time) {
                    it = shard.clients.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

private:
    struct Bucket {
        double tokens = 0.0;
        std::chrono::steady_clock::time_point last_refill{};
    };

    struct Shard {
        std::unordered_map<uint32_t, Bucket> clients;
        std::mutex mutex;
    };

    // Maps a client_id to a specific shard using a simple hash/modulo
    Shard& getShard(uint32_t client_id) {
        return shards_[client_id % num_shards_];
    }

    const double capacity_;
    const double refill_rate_;
    const size_t num_shards_;
    std::vector<Shard> shards_;
};

// ==========================================
// Example Usage / Test Simulation
// ==========================================
int main() {
    // Rule: Allow 5 requests per 2 seconds for any client (using 4 shards)
    RateLimiter limiter(5, 2.0, 4);
    uint32_t client_id = 101;

    std::cout << "Starting improved rate limit test (5 requests / 2 seconds)...\n\n";

    // Simulate sending 7 rapid requests
    for (int i = 1; i <= 7; ++i) {
        bool allowed = limiter.allowRequest(client_id);
        std::cout << "Request " << i << ": " 
                  << (allowed ? "[ ALLOWED ]" : "[ RATE LIMITED ]") << "\n";
    }

    // Wait for refill (sleeping for 1 second to recover some tokens)
    std::cout << "\nWaiting 1 second for token refill...\n";
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Try sending requests again
    for (int i = 8; i <= 10; ++i) {
        bool allowed = limiter.allowRequest(client_id);
        std::cout << "Request " << i << ": " 
                  << (allowed ? "[ ALLOWED ]" : "[ RATE LIMITED ]") << "\n";
    }

    return 0;
}
