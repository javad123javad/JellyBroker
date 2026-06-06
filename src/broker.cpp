#include "broker.h"
#include "logger.h"
#include <iostream>

Broker::Broker()
    : config_(Config::instance())
    , io_()
    , delivery_(io_)
    , signals_(io_) {
}

Broker::~Broker() {
    shutdown();
}

void Broker::run() {
    setup_auth();
    setup_signals();

    BrokerContext ctx;
    ctx.topic_tree = &topic_tree_;
    ctx.sub_manager = &sub_manager_;
    ctx.auth = auth_.get();
    ctx.delivery = &delivery_;

    server_ = std::make_unique<Server>(
        io_, ctx,
        config_.host(), config_.port(),
        config_.max_connections());

    server_->start();
    running_ = true;

    Logger::instance().info("MQTT Broker started on port {} with {} workers",
                            config_.port(), config_.thread_pool_size());

    int pool_size = config_.thread_pool_size();
    if (pool_size < 1) pool_size = 1;

    for (int i = 0; i < pool_size; ++i) {
        threads_.emplace_back([this] { io_.run(); });
    }

    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
}

void Broker::shutdown() {
    if (!running_) return;
    running_ = false;

    Logger::instance().info("Shutting down MQTT Broker...");

    if (server_) {
        server_->stop();
    }

    io_.stop();

    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }

    Logger::instance().info("MQTT Broker stopped");
    Logger::instance().shutdown();
}

void Broker::setup_auth() {
#if HAS_LIBPQXX
    if (config_.auth_backend() == "postgres") {
        auth_ = std::make_unique<PgAuthenticator>(
            config_.pg_connection_string(),
            config_.pg_pool_size());
        Logger::instance().info("Authentication: PostgreSQL");
        return;
    }
#endif
    Logger::instance().warn("Auth backend '{}' unavailable, allowing all connections",
                            config_.auth_backend());
    auth_ = std::make_unique<AllowAllAuthenticator>();
}

void Broker::setup_signals() {
    signals_.add(SIGINT);
    signals_.add(SIGTERM);

    signals_.async_wait([this](const boost::system::error_code& ec, int sig) {
        if (!ec) {
            Logger::instance().info("Received signal {}", sig);
            shutdown();
        }
    });
}
