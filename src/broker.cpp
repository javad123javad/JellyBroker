#include "broker.h"
#include "logger.h"
#include <iostream>
#include <stdexcept>

Broker::Broker()
    : config_(Config::instance())
    , io_()
    , delivery_(io_)
    , signals_(io_) {
}

Broker::~Broker() {
    shutdown();
}

void Broker::update_context() {
    ctx_.topic_tree = &topic_tree_;
    ctx_.sub_manager = &sub_manager_;
    ctx_.auth = auth_.get();
    ctx_.delivery = &delivery_;
    ctx_.server_state = &server_state_;
    ctx_.ssl_ctx = ssl_ctx_.get();
}

void Broker::run() {
    setup_auth();
    setup_tls();
    update_context();

    setup_signals();

    server_ = std::make_unique<Server>(
        io_, ctx_,
        config_.host(), config_.port(),
        config_.max_connections());

    server_->start();
    running_ = true;

    Logger::instance().info("MQTT Broker started on port {} with {} workers",
                            config_.port(), config_.thread_pool_size());

    int pool_size = config_.thread_pool_size();

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
    auto backend = config_.auth_backend();

    if (backend == "allow_all") {
        auth_ = std::make_unique<AllowAllAuthenticator>();
        Logger::instance().warn("Authentication: allow_all (INSECURE - for testing only)");
        return;
    }

#if HAS_LIBPQXX
    if (backend == "postgres") {
        try {
            auth_ = std::make_unique<PgAuthenticator>(
                config_.pg_connection_string(),
                config_.pg_pool_size());
            Logger::instance().info("Authentication: PostgreSQL");
            return;
        } catch (const std::exception& e) {
            Logger::instance().error("Failed to init PgAuthenticator: {}", e.what());
            throw std::runtime_error("Auth backend 'postgres' unavailable: " + std::string(e.what()));
        }
    }
#else
    if (backend == "postgres") {
        throw std::runtime_error("Auth backend 'postgres' requested but libpqxx not available. "
                                 "Recompile with libpqxx or set auth.backend to 'allow_all'.");
    }
#endif

    std::string available = "allow_all";
#if HAS_LIBPQXX
    available += ", postgres";
#endif
    throw std::runtime_error("Unknown auth backend: '" + backend + "'. Available: " + available);
}

void Broker::setup_tls() {
    if (!config_.tls_enabled()) return;

    try {
        ssl_ctx_ = std::make_unique<boost::asio::ssl::context>(
            boost::asio::ssl::context::tlsv12);

        ssl_ctx_->set_options(
            boost::asio::ssl::context::default_workarounds |
            boost::asio::ssl::context::no_sslv2 |
            boost::asio::ssl::context::no_sslv3 |
            boost::asio::ssl::context::single_dh_use);

        ssl_ctx_->use_certificate_file(config_.tls_cert_file(),
                                       boost::asio::ssl::context::pem);
        ssl_ctx_->use_private_key_file(config_.tls_key_file(),
                                       boost::asio::ssl::context::pem);

        if (!config_.tls_ca_file().empty()) {
            ssl_ctx_->load_verify_file(config_.tls_ca_file());
        }

        Logger::instance().info("TLS enabled (cert: {})", config_.tls_cert_file());
    } catch (const std::exception& e) {
        Logger::instance().error("Failed to setup TLS: {}", e.what());
        throw std::runtime_error("TLS setup failed: " + std::string(e.what()));
    }
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
