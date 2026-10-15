#include <iostream>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <thread>
#include <algorithm>
#include <vector>
#include <string>

// Define the Order class/struct
class Order {
public:
    Order(int id, std::string item, int qty, double price)
        : order_id_(id), item_name_(std::move(item)), quantity_(qty), price_(price) {}

    int getOrderId() const { return order_id_; }
    const std::string& getItemName() const { return item_name_; }
    int getQuantity() const { return quantity_; }
    double getPrice() const { return price_; }

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

// Order Manager system that wraps the RateLimiter
class OrderSystem {
public:
    OrderSystem(int max_requests_per_window, double window_seconds)
        : limiter_(max_requests_per_window, window_seconds) {}

    // Send order function checking rate limits per client ID
    bool sendOrder(uint32_t client_id, Order& order) {
        // Check if the client is allowed to place an order
        if (!limiter_.allowRequest(client_id)) {
            std::cout << "[ RATE LIMITED ] Client " << client_id << " tried to send ";
            order.printDetails();
            std::cout << " -> DENIED.\n";
            return false;
        }

        // Process the order successfully
        std::cout << "[ SUCCESS    ] Client " << client_id << " sent ";
        order.printDetails();
        std::cout << " -> ACCEPTED.\n";
        
        // Add actual order placement logic here (e.g., push to queue, write to DB)
        return true;
    }

private:
    RateLimiter limiter_;
};

// ==========================================
// Example Usage / Test Simulation
// ==========================================
int main() {
    // Rule: Allow a client to send up to 3 orders per 2 seconds
    OrderSystem orderSystem(3, 2.0);
    uint32_t client_id = 42;

    std::cout << "Starting Order System Test (3 orders / 2 seconds limit)...\n\n";

    // Simulate sending 5 rapid orders from the same client
    for (int i = 1; i <= 5; ++i) {
        Order order(1000 + i, "Laptop", 1, 999.99);
        orderSystem.sendOrder(client_id, order);
    }

    // Wait 1 second to allow token regeneration
    std::cout << "\nWaiting 1 second for token refill...\n";
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Try sending more orders
    std::cout << "\nSending subsequent orders after partial refill:\n";
    for (int i = 6; i <= 8; ++i) {
        Order order(1000 + i, "Mouse", 2, 49.99);
        orderSystem.sendOrder(client_id, order);
    }

    return 0;
}
