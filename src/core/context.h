#pragma once
#include "topic/topic_tree.h"
#include "subscription.h"
#include "auth/authenticator.h"
#include "core/delivery_engine.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <cstdint>

class Session;

namespace boost::asio::ssl {
class context;
}

struct ServerState {
    std::mutex mutex;
    std::unordered_map<std::string, std::weak_ptr<Session>> active_clients;
    std::unordered_map<std::string, int> auth_failures;
    std::unordered_map<std::string, int64_t> auth_ban_until;
    std::unordered_map<std::string, int> connections_per_ip;
};

struct MessageCounters {
    std::atomic<int64_t> received{0};
    std::atomic<int64_t> published{0};
    std::atomic<int64_t> dropped{0};
    std::atomic<int64_t> subscribed{0};
};

struct BrokerContext {
    TopicTree* topic_tree = nullptr;
    SubscriptionManager* sub_manager = nullptr;
    Authenticator* auth = nullptr;
    DeliveryEngine* delivery = nullptr;
    ServerState* server_state = nullptr;
    boost::asio::ssl::context* ssl_ctx = nullptr;
    MessageCounters* counters = nullptr;
};
