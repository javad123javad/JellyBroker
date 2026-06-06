#pragma once
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct Subscription {
    std::string topic_filter;
    uint8_t qos = 0;
};

class SubscriptionManager {
public:
    void add(const std::string& client_id, const std::string& filter, uint8_t qos);
    void remove(const std::string& client_id, const std::string& filter);
    void remove_client(const std::string& client_id);
    std::vector<Subscription> get_client_subs(const std::string& client_id) const;

private:
    std::unordered_map<std::string, std::vector<Subscription>> subs_;
    mutable std::shared_mutex mutex_;
};
