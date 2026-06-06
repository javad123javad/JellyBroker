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
    int session_expiry_seconds() const;
    double keepalive_multiplier() const;

    std::string log_level() const;
    std::string log_file() const;

    std::string auth_backend() const;
    std::string pg_connection_string() const;
    int pg_pool_size() const;

private:
    Config() = default;
    nlohmann::json json_;
};
