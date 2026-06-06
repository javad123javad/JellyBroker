#pragma once
#include "config.h"
#include "topic/topic_tree.h"
#include "subscription.h"
#include "auth/authenticator.h"
#include "auth/allow_all_authenticator.h"
#if HAS_LIBPQXX
#include "auth/pg_authenticator.h"
#endif
#include "core/delivery_engine.h"
#include "server.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <memory>
#include <thread>
#include <vector>

class Broker {
public:
    Broker();
    ~Broker();

    void run();
    void shutdown();

private:
    void setup_auth();
    void setup_signals();

    Config& config_;
    boost::asio::io_context io_;

    TopicTree topic_tree_;
    SubscriptionManager sub_manager_;
    std::unique_ptr<Authenticator> auth_;
    DeliveryEngine delivery_;

    std::unique_ptr<Server> server_;

    boost::asio::signal_set signals_;
    std::vector<std::thread> threads_;
    bool running_ = false;
};
