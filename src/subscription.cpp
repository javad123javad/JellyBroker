#include "subscription.h"
#include <algorithm>
#include <mutex>

void SubscriptionManager::add(const std::string& client_id, const std::string& filter, uint8_t qos) {
    std::unique_lock lock(mutex_);
    auto& subs = subs_[client_id];
    auto it = std::find_if(subs.begin(), subs.end(),
        [&](const Subscription& s) { return s.topic_filter == filter; });
    if (it != subs.end()) {
        it->qos = qos;
    } else {
        subs.push_back({filter, qos});
    }
}

void SubscriptionManager::remove(const std::string& client_id, const std::string& filter) {
    std::unique_lock lock(mutex_);
    auto it = subs_.find(client_id);
    if (it == subs_.end()) return;
    auto& subs = it->second;
    subs.erase(std::remove_if(subs.begin(), subs.end(),
        [&](const Subscription& s) { return s.topic_filter == filter; }),
        subs.end());
    if (subs.empty()) {
        subs_.erase(it);
    }
}

void SubscriptionManager::remove_client(const std::string& client_id) {
    std::unique_lock lock(mutex_);
    subs_.erase(client_id);
}

std::vector<Subscription> SubscriptionManager::get_client_subs(const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    auto it = subs_.find(client_id);
    if (it == subs_.end()) return {};
    return it->second;
}
