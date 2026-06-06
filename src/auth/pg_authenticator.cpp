#include "pg_authenticator.h"
#include "topic/filter.h"
#include <openssl/evp.h>
#include <sstream>
#include <iomanip>

PgAuthenticator::PgAuthenticator(const std::string& conn_str, int pool_size, int cache_ttl_seconds)
    : conn_str_(conn_str), cache_ttl_(cache_ttl_seconds) {
    for (int i = 0; i < pool_size; ++i) {
        auto pc = std::make_unique<PooledConnection>();
        pc->conn = std::make_unique<pqxx::connection>(conn_str_);
        pc->in_use = false;
        pool_.push_back(std::move(pc));
    }
}

PgAuthenticator::~PgAuthenticator() = default;

PgAuthenticator::PooledConnection* PgAuthenticator::acquire() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    for (auto& pc : pool_) {
        if (!pc->in_use) {
            if (!pc->conn->is_open()) {
                pc->conn = std::make_unique<pqxx::connection>(conn_str_);
            }
            pc->in_use = true;
            return pc.get();
        }
    }
    return nullptr;
}

void PgAuthenticator::release(PooledConnection* pc) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    pc->in_use = false;
}

std::string PgAuthenticator::hash_password(const std::string& password, const std::string& salt) const {
    std::string salted = salt + password;
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, salted.data(), salted.size());
    EVP_DigestFinal_ex(ctx, hash, &hash_len);
    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    for (unsigned int i = 0; i < hash_len; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(hash[i]);
    }
    return oss.str();
}

AuthResult PgAuthenticator::authenticate(const std::string& client_id,
                                         const std::string& username,
                                         const std::string& password) {
    AuthResult result;
    result.success = false;
    result.reason = ConnackCode::REFUSED_NOT_AUTHORIZED;

    auto* pc = acquire();
    if (!pc) return result;

    try {
        pqxx::work txn(*pc->conn);

        auto query = txn.exec_params(
            "SELECT password_hash, salt FROM clients "
            "WHERE client_id = $1 AND enabled = true",
            client_id);

        if (query.empty()) {
            // Return same generic error to prevent client enumeration
            txn.commit();
            release(pc);
            return result;
        }

        auto row = query[0];
        std::string stored_hash = row[0].as<std::string>();
        std::string salt = row[1].as<std::string>();

        std::string computed_hash = hash_password(password, salt);

        if (computed_hash == stored_hash) {
            result.success = true;
            result.reason = ConnackCode::ACCEPTED;
        }
        // Same generic error for wrong password

        txn.commit();
    } catch (const std::exception& e) {
        return result;
    }

    release(pc);
    return result;
}

PgAuthenticator::AclCacheEntry PgAuthenticator::load_acl_rules(const std::string& username) {
    AclCacheEntry entry;

    auto* pc = acquire();
    if (!pc) return entry;

    try {
        pqxx::work txn(*pc->conn);

        auto query = txn.exec_params(
            "SELECT topic_filter, access FROM acls WHERE username = $1",
            username);

        for (const auto& row : query) {
            std::string topic_filter = row[0].as<std::string>();
            int access_level = row[1].as<int>();
            entry.rules.emplace_back(std::move(topic_filter), static_cast<Access>(access_level));
        }

        txn.commit();
    } catch (const std::exception& e) {
        release(pc);
        return entry;
    }

    release(pc);

    if (cache_ttl_ > 0) {
        entry.expiry = std::chrono::steady_clock::now() + std::chrono::seconds(cache_ttl_);
    }

    return entry;
}

Access PgAuthenticator::check_acl(const std::string& username,
                                  const std::string& topic,
                                  Access required) {
    AclCacheEntry entry;

    if (cache_ttl_ > 0) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = acl_cache_.find(username);
        if (it != acl_cache_.end() && std::chrono::steady_clock::now() < it->second.expiry) {
            entry = it->second;
        } else {
            entry = load_acl_rules(username);
            acl_cache_[username] = entry;
        }
    } else {
        entry = load_acl_rules(username);
    }

    Access result = Access::NONE;
    for (const auto& [topic_filter, acc] : entry.rules) {
        if (topic::matches(topic, topic_filter)) {
            if (static_cast<int>(acc) > static_cast<int>(result)) {
                result = acc;
            }
        }
    }

    if (static_cast<int>(result) >= static_cast<int>(required)) {
        return result;
    }

    return Access::NONE;
}
