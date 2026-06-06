#pragma once
#include "topic/topic_tree.h"
#include "subscription.h"
#include "auth/authenticator.h"
#include "core/delivery_engine.h"
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

struct BrokerContext {
    TopicTree* topic_tree = nullptr;
    SubscriptionManager* sub_manager = nullptr;
    Authenticator* auth = nullptr;
    DeliveryEngine* delivery = nullptr;
    ServerState* server_state = nullptr;
    boost::asio::ssl::context* ssl_ctx = nullptr;
};
