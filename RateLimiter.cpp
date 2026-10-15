#include <iostream>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>

class RateLimiter {
public:
    /**
     * @param max_requests (m) Maximum number of requests allowed in the window.
     * @param window_seconds (n) Time window in seconds.
     */
    RateLimiter(int max_requests, double window_seconds)
        : capacity_(static_cast<double>(max_requests)),
          refill_rate_(static_cast<double>(max_requests) / window_seconds) {}

    // Check if a client is allowed to make a request
    bool allowRequest(uint32_t client_id) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto& bucket = clients_[client_id];

        // If this is a new client, initialize their bucket with full capacity
        if (bucket.last_refill == std::chrono::steady_clock::time_point{}) {
            bucket.tokens = capacity_;
            bucket.last_refill = now;
        }

        // 1. Calculate time elapsed since the last refill
        std::chrono::duration<double> elapsed = now - bucket.last_refill;
        bucket.last_refill = now;

        // 2. Refill tokens based on elapsed time and refill rate
        // We cap the tokens at max capacity to prevent token hoarding
        bucket.tokens = std::min(capacity_, bucket.tokens + (elapsed.count() * refill_rate_));

        // 3. Consume a token if available
        if (bucket.tokens >= 1.0) {
            bucket.tokens -= 1.0;
            return true; // Request allowed
        }

        return false; // Rate limited
    }

    // Prevents memory leaks by clearing out clients who haven't made requests recently
    void cleanupInactiveClients(std::chrono::seconds max_inactive_time) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = std::chrono::steady_clock::now();

        for (auto it = clients_.begin(); it != clients_.end(); ) {
            std::chrono::duration<double> inactive_duration = now - it->second.last_refill;
            if (inactive_duration > max_inactive_time) {
                it = clients_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    struct Bucket {
        double tokens = 0.0;
        std::chrono::steady_clock::time_point last_refill{};
    };

    const double capacity_;     // Maximum tokens (m)
    const double refill_rate_;  // Tokens added per second (m / n)
    std::unordered_map<uint32_t, Bucket> clients_;
    std::mutex mutex_;
};

// ==========================================
// Example Usage / Test Simulation
// ==========================================
int main() {
    // Rule: Allow 5 requests per 2 seconds for any client
    RateLimiter limiter(5, 2.0);
    uint32_t client_id = 101;

    std::cout << "Starting rate limit test (5 requests / 2 seconds)...\n\n";

    // Simulate sending 7 rapid requests
    for (int i = 1; i <= 7; ++i) {
        bool allowed = limiter.allowRequest(client_id);
        std::cout << "Request " << i << ": " 
                  << (allowed ? "[ ALLOWED ]" : "[ RATE LIMITED ]") << "\n";
    }

    // Wait for refill (e.g., sleeping for 1 second to recover some tokens)
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
