#include "config.h"
#include <stdexcept>
#include <fstream>
#include <algorithm>

Config::Config() {
    json_ = nlohmann::json::object();
}

Config& Config::instance() {
    static Config cfg;
    return cfg;
}

void Config::load(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + path);
    }
    file >> json_;

    if (!is_valid()) {
        throw std::runtime_error("Config validation failed: " + validation_error_);
    }
}

std::string Config::host() const {
    return json_.value("broker", nlohmann::json::object()).value("host", "0.0.0.0");
}

uint16_t Config::port() const {
    return json_.value("broker", nlohmann::json::object()).value("port", 1883);
}

int Config::thread_pool_size() const {
    return std::clamp(
        json_.value("broker", nlohmann::json::object()).value("thread_pool_size", 4),
        1, 128);
}

int Config::max_connections() const {
    return json_.value("broker", nlohmann::json::object()).value("max_connections", 100000);
}

int Config::max_connections_per_ip() const {
    return json_.value("broker", nlohmann::json::object()).value("max_connections_per_ip", 100);
}

int Config::session_expiry_seconds() const {
    return json_.value("broker", nlohmann::json::object()).value("session_expiry_seconds", 86400);
}

double Config::keepalive_multiplier() const {
    return json_.value("broker", nlohmann::json::object()).value("keepalive_multiplier", 1.5);
}

int Config::max_keepalive() const {
    return json_.value("broker", nlohmann::json::object()).value("max_keepalive", 3600);
}

int Config::max_packet_size() const {
    return json_.value("broker", nlohmann::json::object()).value("max_packet_size", 262144);
}

int Config::max_topic_depth() const {
    return json_.value("broker", nlohmann::json::object()).value("max_topic_depth", 128);
}

int Config::max_topic_length() const {
    return json_.value("broker", nlohmann::json::object()).value("max_topic_length", 65536);
}

int Config::max_subscriptions_per_client() const {
    return json_.value("broker", nlohmann::json::object()).value("max_subscriptions_per_client", 1000);
}

int Config::max_retained_messages() const {
    return json_.value("broker", nlohmann::json::object()).value("max_retained_messages", 10000);
}

int Config::max_write_queue_size() const {
    return json_.value("broker", nlohmann::json::object()).value("max_write_queue_size", 10000);
}

int Config::max_parser_buffer_size() const {
    return json_.value("broker", nlohmann::json::object()).value("max_parser_buffer_size", 1048576);
}

int Config::max_auth_attempts() const {
    return json_.value("broker", nlohmann::json::object()).value("max_auth_attempts", 10);
}

int Config::auth_ban_seconds() const {
    return json_.value("broker", nlohmann::json::object()).value("auth_ban_seconds", 60);
}

int Config::retry_interval_seconds() const {
    return json_.value("broker", nlohmann::json::object()).value("retry_interval_seconds", 5);
}

int Config::max_retry_count() const {
    return json_.value("broker", nlohmann::json::object()).value("max_retry_count", 5);
}

std::string Config::log_level() const {
    return json_.value("logging", nlohmann::json::object()).value("level", "info");
}

std::string Config::log_file() const {
    return json_.value("logging", nlohmann::json::object()).value("file", "");
}

std::string Config::auth_backend() const {
    return json_.value("auth", nlohmann::json::object()).value("backend", "allow_all");
}

std::string Config::pg_connection_string() const {
    return json_.value("auth", nlohmann::json::object())
               .value("postgres", nlohmann::json::object())
               .value("connection_string", "");
}

int Config::pg_pool_size() const {
    return json_.value("auth", nlohmann::json::object())
               .value("postgres", nlohmann::json::object())
               .value("pool_size", 4);
}

int Config::acl_cache_ttl() const {
    return json_.value("auth", nlohmann::json::object())
               .value("acl_cache_ttl", 60);
}

bool Config::tls_enabled() const {
    return json_.value("tls", nlohmann::json::object()).value("enabled", false);
}

std::string Config::tls_cert_file() const {
    return json_.value("tls", nlohmann::json::object()).value("cert_file", "");
}

std::string Config::tls_key_file() const {
    return json_.value("tls", nlohmann::json::object()).value("key_file", "");
}

std::string Config::tls_ca_file() const {
    return json_.value("tls", nlohmann::json::object()).value("ca_file", "");
}

bool Config::is_valid() const {
    auto port_val = port();
    if (port_val == 0) {
        validation_error_ = "port must not be 0";
        return false;
    }

    auto pool_size = thread_pool_size();
    if (pool_size < 1 || pool_size > 128) {
        validation_error_ = "thread_pool_size must be between 1 and 128";
        return false;
    }

    auto mkeep = max_keepalive();
    if (mkeep < 10 || mkeep > 65535) {
        validation_error_ = "max_keepalive must be between 10 and 65535";
        return false;
    }

    auto mps = max_packet_size();
    if (mps < 256 || mps > 268435455) {
        validation_error_ = "max_packet_size must be between 256 and 268435455";
        return false;
    }

    if (tls_enabled()) {
        if (tls_cert_file().empty()) {
            validation_error_ = "tls.cert_file is required when TLS is enabled";
            return false;
        }
        if (tls_key_file().empty()) {
            validation_error_ = "tls.key_file is required when TLS is enabled";
            return false;
        }
    }

    auto cache_ttl = acl_cache_ttl();
    if (cache_ttl < 0) {
        validation_error_ = "acl_cache_ttl must be >= 0 (0 disables caching)";
        return false;
    }

    if (admin_enabled()) {
        auto admin_port_val = admin_port();
        if (admin_port_val == 0) {
            validation_error_ = "admin.port must not be 0 when admin is enabled";
            return false;
        }
    }

    return true;
}

bool Config::mdns_enabled() const {
    return json_.value("mdns", nlohmann::json::object()).value("enabled", false);
}

bool Config::admin_enabled() const {
    return json_.value("admin", nlohmann::json::object()).value("enabled", false);
}

uint16_t Config::admin_port() const {
    return json_.value("admin", nlohmann::json::object()).value("port", 1884);
}

bool Config::admin_tls_enabled() const {
    return json_.value("admin", nlohmann::json::object()).value("tls_enabled", false);
}

std::string Config::validation_error() const {
    return validation_error_;
}
