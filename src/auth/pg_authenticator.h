#pragma once
#include "authenticator.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#if HAS_LIBPQXX
#include <pqxx/pqxx>

class PgAuthenticator : public Authenticator {
public:
    PgAuthenticator(const std::string& conn_str, int pool_size, int cache_ttl_seconds);
    ~PgAuthenticator() override;

    AuthResult authenticate(const std::string& client_id,
                            const std::string& username,
                            const std::string& password) override;

    Access check_acl(const std::string& username,
                     const std::string& topic,
                     Access required) override;

private:
    struct PooledConnection {
        std::unique_ptr<pqxx::connection> conn;
        bool in_use = false;
    };

    struct AclCacheEntry {
        std::vector<std::pair<std::string, Access>> rules;
        std::chrono::steady_clock::time_point expiry;
    };

    std::string conn_str_;
    std::vector<std::unique_ptr<PooledConnection>> pool_;
    std::mutex pool_mutex_;
    int cache_ttl_;

    std::unordered_map<std::string, AclCacheEntry> acl_cache_;
    std::mutex cache_mutex_;

    PooledConnection* acquire();
    void release(PooledConnection* pc);

    AclCacheEntry load_acl_rules(const std::string& username);

    std::string hash_password(const std::string& password, const std::string& salt) const;
};
#endif
