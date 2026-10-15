#include <iostream>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>
#include <vector>
#include <string>
#include <atomic>

class Order {
public:
    Order(int id, std::string item, int qty, double price)
        : order_id_(id), item_name_(std::move(item)), quantity_(qty), price_(price) {}

    void printDetails() const {
        std::cout << "Order #" << order_id_ << " [" << item_name_ 
                  << " x " << quantity_ << " @ $" << price_ << "]";
    }

private:
    int order_id_;
    std::string item_name_;
    int quantity_;
    double price_;
};

class RateLimiter {
public:
    RateLimiter(int max_requests, double window_seconds, size_t num_shards = 16)
        : capacity_(static_cast<double>(max_requests)),
          refill_rate_(static_cast<double>(max_requests) / window_seconds),
          num_shards_(num_shards),
          shards_(num_shards) {}

    bool allowRequest(uint32_t client_id) {
        auto& shard = getShard(client_id);
        std::lock_guard<std::mutex> lock(shard.mutex);

        auto now = std::chrono::steady_clock::now();
        auto& bucket = shard.clients[client_id];

        if (bucket.last_refill == std::chrono::steady_clock::time_point{}) {
            bucket.tokens = capacity_ - 1.0;
            bucket.last_refill = now;
            return true;
        }

        std::chrono::duration<double> elapsed = now - bucket.last_refill;
        bucket.last_refill = now;

        bucket.tokens = std::min(capacity_, bucket.tokens + (elapsed.count() * refill_rate_));

        if (bucket.tokens >= 1.0) {
            bucket.tokens -= 1.0;
            return true; 
        }

        return false; 
    }

    // Active cleanup function to prevent memory leaks
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

    Shard& getShard(uint32_t client_id) {
        return shards_[client_id % num_shards_];
    }

    const double capacity_;
    const double refill_rate_;
    const size_t num_shards_;
    std::vector<Shard> shards_;
};

class OrderSystem {
public:
    OrderSystem(int max_requests_per_window, double window_seconds)
        : limiter_(max_requests_per_window, window_seconds), running_(true) {
        
        // Start a background thread to clean up inactive clients every 10 seconds
        cleanup_thread_ = std::thread([this]() {
            while (running_) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                // Remove clients inactive for more than 30 seconds (for testing purposes)
                limiter_.cleanupInactiveClients(std::chrono::seconds(30));
                std::cout << "[SYSTEM] Cleaned up inactive rate limiter clients.\n";
            }
        });
    }

    ~OrderSystem() {
        running_ = false;
        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }
    }

    bool sendOrder(uint32_t client_id, Order& order) {
        if (!limiter_.allowRequest(client_id)) {
            std::cout << "[ RATE LIMITED ] Client " << client_id << " tried to send ";
            order.printDetails();
            std::cout << " -> DENIED.\n";
            return false;
        }

        std::cout << "[ SUCCESS    ] Client " << client_id << " sent ";
        order.printDetails();
        std::cout << " -> ACCEPTED.\n";
        return true;
    }

private:
    RateLimiter limiter_;
    std::thread cleanup_thread_;
    std::atomic<bool> running_;
};

int main() {
    OrderSystem orderSystem(3, 2.0);
    uint32_t client_id = 42;

    Order order(1001, "Laptop", 1, 999.99);
    orderSystem.sendOrder(client_id, order);

    // Let it run briefly to show the system works
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return 0;
}
