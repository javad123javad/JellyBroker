#include "config.h"
#include <stdexcept>
#include <fstream>

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
}

std::string Config::host() const {
    return json_.value("broker", nlohmann::json::object()).value("host", "0.0.0.0");
}

uint16_t Config::port() const {
    return json_.value("broker", nlohmann::json::object()).value("port", 1883);
}

int Config::thread_pool_size() const {
    return json_.value("broker", nlohmann::json::object()).value("thread_pool_size", 4);
}

int Config::max_connections() const {
    return json_.value("broker", nlohmann::json::object()).value("max_connections", 100000);
}

int Config::session_expiry_seconds() const {
    return json_.value("broker", nlohmann::json::object()).value("session_expiry_seconds", 86400);
}

double Config::keepalive_multiplier() const {
    return json_.value("broker", nlohmann::json::object()).value("keepalive_multiplier", 1.5);
}

std::string Config::log_level() const {
    return json_.value("logging", nlohmann::json::object()).value("level", "info");
}

std::string Config::log_file() const {
    return json_.value("logging", nlohmann::json::object()).value("file", "");
}

std::string Config::auth_backend() const {
    return json_.value("auth", nlohmann::json::object()).value("backend", "postgres");
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
