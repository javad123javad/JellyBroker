#pragma once
#include <nlohmann/json.hpp>
#include <string>

class Config {
public:
    static Config& instance();
    void load(const std::string& path);

    std::string host() const;
    uint16_t port() const;
    int thread_pool_size() const;
    int max_connections() const;
    int max_connections_per_ip() const;
    int session_expiry_seconds() const;
    double keepalive_multiplier() const;
    int max_keepalive() const;
    int max_packet_size() const;
    int max_topic_depth() const;
    int max_topic_length() const;
    int max_subscriptions_per_client() const;
    int max_retained_messages() const;
    int max_write_queue_size() const;
    int max_parser_buffer_size() const;
    int max_auth_attempts() const;
    int auth_ban_seconds() const;
    int retry_interval_seconds() const;
    int max_retry_count() const;

    std::string log_level() const;
    std::string log_file() const;

    std::string auth_backend() const;
    std::string pg_connection_string() const;
    int pg_pool_size() const;

    bool tls_enabled() const;
    std::string tls_cert_file() const;
    std::string tls_key_file() const;
    std::string tls_ca_file() const;

    bool is_valid() const;
    std::string validation_error() const;

private:
    Config();
    nlohmann::json json_;
    mutable std::string validation_error_;
};
